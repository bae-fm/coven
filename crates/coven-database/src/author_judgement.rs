//! The author's judgement of its own write (§9.6): the check every receiver
//! makes, run before the write is queued. A change receivers would refuse
//! refuses the whole local transaction, and the same judgement records each
//! change's verdict.

use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::loss::AuthorizationFailure;
use coven_format::snapshot_rows::ChangeVerdict;
use coven_format::write::WriteRecord;

/// Judge a queued write's retained changes, refuse it if any change fails,
/// and record each change's verdict otherwise.
pub(crate) fn judge_own_write(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    record: &WriteRecord,
) -> Result<(), DbError> {
    let judgement = crate::judge::judge(database, schema, &record.header)?;
    #[cfg(test)]
    database.judged((record.clone(), judgement.clone()));
    // A refused action follows from its root, so name a refused direct
    // change first: a row the app's own SQL changed.
    let refused = judgement
        .rows
        .iter()
        .filter_map(|(row, checked)| Some((row, checked.derived, checked.check.as_ref().err()?)))
        .min_by_key(|(_, derived, _)| *derived);
    if let Some((row, _, failure)) = refused {
        let (table, key) = (row.table.clone(), crate::RowKey::decode(&row.key)?);
        return Err(match failure {
            AuthorizationFailure::NotOwner { owner } => DbError::NotOwner {
                table,
                key,
                owner: *owner,
            },
            AuthorizationFailure::InvalidForeignKeyEffect => {
                DbError::InvalidForeignKeyEffect { table, key }
            }
        });
    }
    for (row, checked) in &judgement.rows {
        let verdict = if checked.derived {
            ChangeVerdict::Derived(checked.causes(record.header.position))
        } else {
            ChangeVerdict::Applied
        };
        crate::write_sources::admit(database, &record.header, row, &verdict)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "author_judgement_tests.rs"]
pub(crate) mod tests;
