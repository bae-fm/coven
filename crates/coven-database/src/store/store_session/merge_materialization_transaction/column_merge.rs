use super::*;
use rusqlite::session::{Changegroup, Session};

/// SQLite encodes column corrections in an isolated row table. This table has
/// the changeset's column and primary-key layout; application constraints belong
/// to the live database, where the complete combined changeset is applied once.
pub(super) struct ColumnMergeEncoder {
    connection: Connection,
    changes: Changegroup,
    tables: HashSet<String>,
}

impl ColumnMergeEncoder {
    pub(super) fn new(original: &[u8]) -> Result<Self, DbError> {
        let mut changes = Changegroup::new()?;
        changes.add_stream(&mut &original[..])?;
        Ok(Self {
            connection: Connection::open_in_memory()?,
            changes,
            tables: HashSet::new(),
        })
    }

    pub(super) fn record(
        &mut self,
        table: &str,
        columns: &[String],
        incoming: &[Value],
        merged: &[Value],
        indirect: bool,
    ) -> Result<(), DbError> {
        let quoted_table = quote_ident(table);
        if self.tables.insert(table.to_string()) {
            let definitions = columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    if index == 0 {
                        format!("{} PRIMARY KEY NOT NULL", quote_ident(column))
                    } else {
                        quote_ident(column)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            self.connection
                .execute_batch(&format!("CREATE TABLE {quoted_table} ({definitions})"))?;
        }
        let placeholders = (1..=columns.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        self.connection.execute(
            &format!("INSERT INTO {quoted_table} VALUES ({placeholders})"),
            params_from_iter(incoming),
        )?;
        let mut session = Session::new(&self.connection)?;
        session.attach(Some(table))?;
        session.set_indirect(indirect);
        let assignments = columns
            .iter()
            .enumerate()
            .skip(1)
            .map(|(index, column)| format!("{} = ?{}", quote_ident(column), index + 1))
            .collect::<Vec<_>>()
            .join(", ");
        self.connection.execute(
            &format!(
                "UPDATE {quoted_table} SET {assignments} WHERE {} = ?1",
                quote_ident(&columns[0])
            ),
            params_from_iter(merged),
        )?;
        self.changes.add(&session.changeset()?)?;
        drop(session);
        self.connection
            .execute(&format!("DELETE FROM {quoted_table}"), [])?;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Vec<u8>, DbError> {
        let mut bytes = Vec::new();
        self.changes.output_strm(&mut bytes)?;
        Ok(bytes)
    }
}
