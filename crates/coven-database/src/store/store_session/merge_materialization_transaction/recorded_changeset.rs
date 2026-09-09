use super::*;
use coven_protocol::write::{AffectedRow, WriteId, WriteRebaseConflict, WriteRebaseConflictReason};

impl MergeMaterializationTransaction<'_, '_> {
    /// Validate the recorded sparse intent against accepted rows, then use SQLite's
    /// changeset application (including constraint retries) to realize it. The
    /// owner holds ReplaySql while capturing these effects, validates the resulting
    /// foreign-key graph, and commits the replacement base with the rows. Any
    /// error requires rollback of the enclosing replay transaction.
    pub(crate) fn apply_recorded_changeset<B: AsRef<[u8]>>(
        &self,
        changeset: ValidatedChangeset<B>,
        write_id: &WriteId,
        stamp: &Timestamp,
    ) -> Result<(), DbError> {
        let bytes = changeset.bytes();
        if bytes.is_empty() {
            return Ok(());
        }
        let conn = self.store.transaction;
        let input: &mut dyn std::io::Read = &mut &bytes[..];
        let mut iter = ChangesetIter::start_strm(&input)?;
        let mut restamp = Vec::new();
        let mut affected_rows = Vec::new();
        while let Some(item) = iter.next()? {
            let row = recorded_row(item)?;
            let columns = recorded_columns(item, changeset.schema())?;
            let current = conn
                .query_row(
                    &format!(
                        "SELECT {} FROM {} WHERE {} = ?1",
                        columns
                            .iter()
                            .map(|column| quote_ident(column))
                            .collect::<Vec<_>>()
                            .join(", "),
                        quote_ident(&row.table),
                        quote_ident(&columns[0])
                    ),
                    [&row.primary_key],
                    |result| {
                        (0..columns.len())
                            .map(|index| result.get::<_, Value>(index))
                            .collect::<rusqlite::Result<Vec<_>>>()
                    },
                )
                .optional()?;
            validate_recorded_values(item, changeset.schema(), current.as_deref(), write_id)?;
            affected_rows.push(row.clone());
            if item.op()?.code() != Action::SQLITE_DELETE {
                restamp.push(row);
            }
        }
        drop(iter);

        let failure = Arc::new(Mutex::new(None));
        let callback_failure = failure.clone();
        let schema = changeset.schema.clone();
        let callback_write = write_id.clone();
        let applied = conn.apply_strm(
            &mut &bytes[..],
            None::<fn(&str) -> bool>,
            move |kind, item| {
                // This iterator has no row for FOREIGN_KEY. ReplaySql disables
                // FK actions for captured effects and the owner checks the final
                // graph; an unexpected native FK conflict must still abort.
                if kind == ConflictType::SQLITE_CHANGESET_FOREIGN_KEY {
                    return ConflictAction::SQLITE_CHANGESET_ABORT;
                }
                match recorded_conflict(kind, &item, &schema, &callback_write) {
                    Ok(action) => action,
                    Err(error) => {
                        *callback_failure
                            .lock()
                            .expect("recorded conflict mutex poisoned") = Some(error);
                        ConflictAction::SQLITE_CHANGESET_ABORT
                    }
                }
            },
        );
        if let Some(error) = failure
            .lock()
            .expect("recorded conflict mutex poisoned")
            .take()
        {
            return Err(error);
        }
        applied.map_err(|error| {
            if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
                // SQLite's deferred constraint retry can fail without invoking
                // the row-conflict callback. Attribute that rejection to the
                // captured write, not the last row seen by a callback.
                DbError::from(WriteRebaseConflict {
                    write_id: write_id.clone(),
                    affected_rows,
                    reason: WriteRebaseConflictReason::Constraint {
                        message: error.to_string(),
                    },
                })
            } else {
                DbError::from(error)
            }
        })?;
        for row in restamp {
            let columns = changeset.schema().columns(&row.table).ok_or_else(|| {
                DbError::Message(format!(
                    "recorded row table {:?} has no column map",
                    row.table
                ))
            })?;
            let updated = conn
                .execute(
                    &format!(
                        "UPDATE {} SET _updated_at = ?1 WHERE {} = ?2",
                        quote_ident(&row.table),
                        quote_ident(&columns[0])
                    ),
                    rusqlite::params![stamp.to_string(), row.primary_key],
                )
                .map_err(|error| {
                    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
                        rebase_conflict(
                            write_id,
                            row.clone(),
                            WriteRebaseConflictReason::Constraint {
                                message: error.to_string(),
                            },
                        )
                    } else {
                        DbError::from(error)
                    }
                })?;
            if updated != 1 {
                return Err(rebase_conflict(
                    write_id,
                    row,
                    WriteRebaseConflictReason::MissingTarget,
                ));
            }
        }
        Ok(())
    }
}

fn recorded_row(item: &ChangesetItem) -> Result<AffectedRow, DbError> {
    let op = item.op()?;
    let side = match op.code() {
        Action::SQLITE_INSERT => UpdateValue::New,
        Action::SQLITE_UPDATE | Action::SQLITE_DELETE => UpdateValue::Old,
        code => {
            return Err(DbError::Message(format!(
                "unsupported recorded edit {code:?}"
            )));
        }
    };
    Ok(AffectedRow {
        table: op.table_name().to_string(),
        primary_key: required_text_changeset_value(item, op.table_name(), 0, side, "row id")?,
    })
}

fn recorded_columns<'schema>(
    item: &ChangesetItem,
    schema: &'schema TableSchema,
) -> Result<&'schema [String], DbError> {
    let op = item.op()?;
    let table = op.table_name();
    let columns = schema.columns(table).ok_or_else(|| {
        DbError::Message(format!("recorded edit contains undeclared table {table:?}"))
    })?;
    if op.number_of_columns() as usize != columns.len() {
        return Err(DbError::Message(format!(
            "recorded edit for {table:?} differs from the current column layout"
        )));
    }
    Ok(columns)
}

fn rebase_conflict(
    write_id: &WriteId,
    row: AffectedRow,
    reason: WriteRebaseConflictReason,
) -> DbError {
    WriteRebaseConflict {
        write_id: write_id.clone(),
        affected_rows: vec![row],
        reason,
    }
    .into()
}

fn validate_recorded_values(
    item: &ChangesetItem,
    schema: &TableSchema,
    current: Option<&[Value]>,
    write_id: &WriteId,
) -> Result<(), DbError> {
    let row = recorded_row(item)?;
    let columns = recorded_columns(item, schema)?;
    let updated_at = schema.updated_at(&row.table).ok_or_else(|| {
        DbError::Message(format!(
            "recorded edit table {:?} has no timestamp column",
            row.table
        ))
    })?;
    let conflict = |reason| rebase_conflict(write_id, row.clone(), reason);
    match (item.op()?.code(), current) {
        (Action::SQLITE_INSERT, None) => Ok(()),
        (Action::SQLITE_INSERT, Some(_)) => {
            Err(conflict(WriteRebaseConflictReason::IdentityCollision))
        }
        (Action::SQLITE_UPDATE | Action::SQLITE_DELETE, None) => {
            Err(conflict(WriteRebaseConflictReason::MissingTarget))
        }
        (Action::SQLITE_UPDATE, Some(current)) => {
            for column in changed_update_columns(item, updated_at)? {
                if current[column.index] != column.base && current[column.index] != column.incoming
                {
                    return Err(conflict(WriteRebaseConflictReason::ChangedColumn {
                        column: columns[column.index].clone(),
                    }));
                }
            }
            Ok(())
        }
        (Action::SQLITE_DELETE, Some(current)) => {
            for (index, value) in current.iter().enumerate() {
                if index == updated_at {
                    continue;
                }
                let base = changeset_value(item, index, UpdateValue::Old)?.ok_or_else(|| {
                    DbError::Message(format!(
                        "recorded DELETE for {:?} lacks column {index}",
                        row.table
                    ))
                })?;
                if *value != base {
                    return Err(conflict(WriteRebaseConflictReason::ChangedColumn {
                        column: columns[index].clone(),
                    }));
                }
            }
            Ok(())
        }
        (code, _) => Err(DbError::Message(format!(
            "unsupported recorded edit {code:?}"
        ))),
    }
}

fn recorded_conflict(
    kind: ConflictType,
    item: &ChangesetItem,
    schema: &TableSchema,
    write_id: &WriteId,
) -> Result<ConflictAction, DbError> {
    if kind == ConflictType::SQLITE_CHANGESET_DATA {
        let columns = recorded_columns(item, schema)?;
        let current = (0..columns.len())
            .map(|index| Value::try_from(item.conflict(index)?).map_err(DbError::from))
            .collect::<Result<Vec<_>, DbError>>()?;
        validate_recorded_values(item, schema, Some(&current), write_id)?;
        return Ok(ConflictAction::SQLITE_CHANGESET_REPLACE);
    }
    let reason = match kind {
        ConflictType::SQLITE_CHANGESET_NOTFOUND => WriteRebaseConflictReason::MissingTarget,
        ConflictType::SQLITE_CHANGESET_CONFLICT => WriteRebaseConflictReason::IdentityCollision,
        ConflictType::SQLITE_CHANGESET_CONSTRAINT => WriteRebaseConflictReason::Constraint {
            message: "recorded operation violates a database constraint".to_string(),
        },
        other => {
            return Err(DbError::Message(format!(
                "unexpected recorded changeset conflict {other:?}"
            )));
        }
    };
    Err(rebase_conflict(write_id, recorded_row(item)?, reason))
}
