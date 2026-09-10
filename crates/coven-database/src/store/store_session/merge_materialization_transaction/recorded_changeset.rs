use super::*;
use coven_protocol::write::{AffectedRow, WriteId, WriteRebaseConflict, WriteRebaseConflictReason};

impl MergeMaterializationTransaction<'_, '_> {
    /// Use ordinary merge rules and captured timestamps for unpublished work.
    /// Attribute constraint failures to its write; the replay owner rolls back
    /// the entire suffix and validates its final foreign-key graph.
    pub(crate) fn apply_recorded_changeset<B: AsRef<[u8]>>(
        &self,
        changeset: ValidatedChangeset<B>,
        write_id: &WriteId,
    ) -> Result<(), DbError> {
        let affected_rows = incoming_rows(changeset.bytes(), changeset.schema())?
            .into_iter()
            .map(|row| AffectedRow {
                table: row.table,
                primary_key: row.row_id,
            })
            .collect::<Vec<_>>();
        let conflict = |message| {
            DbError::from(WriteRebaseConflict {
                write_id: write_id.clone(),
                affected_rows: affected_rows.clone(),
                reason: WriteRebaseConflictReason::Constraint { message },
            })
        };
        let applied = self
            .apply_changeset(changeset, IncomingTimestampPolicy::LocallyAuthored)
            .map_err(|error| match &error {
                DbError::Sqlite(source)
                    if source.sqlite_error_code()
                        == Some(rusqlite::ErrorCode::ConstraintViolation) =>
                {
                    conflict(source.to_string())
                }
                _ => error,
            })?;
        if !applied.constraint_conflict_tables.is_empty() {
            return Err(conflict(format!(
                "merged edit violates constraints in {:?}",
                applied.constraint_conflict_tables,
            )));
        }
        Ok(())
    }
}
