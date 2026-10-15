//! Derive received values from the migration's retained schema identities.

use crate::{
    migration::WriteConversion, migration_names::MigrationMatch, schema::Schema,
    sqlite::DatabaseConnection, DbError, Migration, RowChange,
};
use coven_format::write::WritePart;
use rusqlite::params;
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct ReceivedMigration {
    before: Schema,
    after: Schema,
    names: MigrationMatch,
    conversion: Option<Arc<WriteConversion>>,
}

pub(crate) fn retain(
    db: &DatabaseConnection,
    version: u32,
    before: &Schema,
    after: &Schema,
    names: &MigrationMatch,
) -> Result<(), DbError> {
    let tables = serde_json::to_vec(&names.tables).expect("string map");
    let columns =
        serde_json::to_vec(&names.columns.iter().collect::<Vec<_>>()).expect("string pairs");
    db.internal_execute(
        "INSERT INTO _coven_conversion_schemas VALUES(?1,?2,?3,?4,?5)",
        params![version, before.encode(), after.encode(), tables, columns],
    )?;
    Ok(())
}

pub(crate) fn read(
    db: &DatabaseConnection,
    migrations: &[Migration],
) -> Result<BTreeMap<u32, ReceivedMigration>, DbError> {
    let steps = db.query(
        "SELECT version,before_schema,after_schema,tables,columns FROM _coven_conversion_schemas ORDER BY version",
        [], |r| {
            let decode = |index| -> rusqlite::Result<serde_json::Value> {
                serde_json::from_slice(&r.get::<_, Vec<u8>>(index)?).map_err(|error|
                    crate::write_encoding::decode_failure(index, rusqlite::types::Type::Blob, error))
            };
            let tables = serde_json::from_value(decode(3)?).map_err(|error|
                crate::write_encoding::decode_failure(3, rusqlite::types::Type::Blob, error))?;
            let columns: Vec<((String, String), String)> = serde_json::from_value(decode(4)?).map_err(|error|
                crate::write_encoding::decode_failure(4, rusqlite::types::Type::Blob, error))?;
            Ok((r.get::<_,u32>(0)?, Schema::decode(&r.get::<_,Vec<u8>>(1)?)?,
                Schema::decode(&r.get::<_,Vec<u8>>(2)?)?, MigrationMatch { tables, columns: columns.into_iter().collect() }))
        },
    )?;
    steps
        .into_iter()
        .map(|(version, before, after, names)| {
            let migration = migrations
                .iter()
                .find(|m| m.version == version)
                .ok_or(DbError::DamagedDatabase)?;
            Ok((
                version,
                ReceivedMigration {
                    before,
                    after,
                    names,
                    conversion: migration.conversion.clone(),
                },
            ))
        })
        .collect()
}

/// The schema a write at `version` was authored under, when a breaking
/// migration since then retained it: additions alone never change an
/// existing table's references, so the next retained one serves.
pub(crate) fn authored(steps: &BTreeMap<u32, ReceivedMigration>, version: u32) -> Option<&Schema> {
    steps
        .range((
            std::ops::Bound::Excluded(version),
            std::ops::Bound::Unbounded,
        ))
        .next()
        .map(|(_, step)| &step.before)
}

pub(crate) fn available(steps: &BTreeMap<u32, ReceivedMigration>, from: u32, to: u32) -> bool {
    let mut steps = steps
        .range((
            std::ops::Bound::Excluded(from),
            std::ops::Bound::Included(to),
        ))
        .peekable();
    steps.peek().is_some() && steps.all(|(_, step)| step.conversion.is_some())
}

pub(crate) fn convert(
    steps: &BTreeMap<u32, ReceivedMigration>,
    from: u32,
    to: u32,
    part: &mut WritePart,
) -> Result<(), DbError> {
    for (_, step) in steps.range((
        std::ops::Bound::Excluded(from),
        std::ops::Bound::Included(to),
    )) {
        let conversion = step
            .conversion
            .as_ref()
            .expect("admission checked converters");
        for row in &mut part.rows {
            let mut change = RowChange::read(row, &step.before)?;
            conversion(&mut change)?;
            *row = change.write(&step.after, &step.names)?;
        }
    }
    part.rows.sort_by(|a, b| a.row.cmp(&b.row));
    Ok(())
}
