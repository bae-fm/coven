//! Derive received values from the migration's retained schema identities.

use crate::{
    migration::MigrationKind, migration_names::MigrationMatch, schema::Schema,
    sqlite::DatabaseConnection, DbError, Migration, OldWrites, RowChange,
};
use coven_format::write::WritePart;
use rusqlite::params;
use std::collections::BTreeMap;

pub(crate) struct ReceivedMigration {
    before: Schema,
    after: Schema,
    names: MigrationMatch,
    old_writes: OldWrites,
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
            let MigrationKind::Breaking(old_writes) = &migration.kind else {
                return Err(DbError::MigrationKindMismatch { version });
            };
            Ok((
                version,
                ReceivedMigration {
                    before,
                    after,
                    names,
                    old_writes: old_writes.clone(),
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

/// Every crossed breaking step needs a converter. Only breaking steps are
/// retained; an empty range passes values unchanged, as proved by the queue
/// model's `Chain.additions_pass_unchanged` and `Chain.chain_available_iff`.
pub(crate) fn available(steps: &BTreeMap<u32, ReceivedMigration>, from: u32, to: u32) -> bool {
    steps
        .range((
            std::ops::Bound::Excluded(from),
            std::ops::Bound::Included(to),
        ))
        .all(|(_, step)| matches!(step.old_writes, OldWrites::Convert(_)))
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
        let OldWrites::Convert(conversion) = &step.old_writes else {
            unreachable!("admission checked old-write policies");
        };
        for row in &mut part.rows {
            let mut change = RowChange::read(row, &step.before)?;
            conversion(&mut change)?;
            *row = change.write(&step.after, &step.names)?;
        }
    }
    part.rows.sort_by(|a, b| a.row.cmp(&b.row));
    Ok(())
}

#[cfg(test)]
#[path = "migration_receive_tests.rs"]
mod tests;
