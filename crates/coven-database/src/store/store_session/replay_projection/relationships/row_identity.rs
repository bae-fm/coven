use super::*;

/// Native column renames expose a physical rowid without changing it. The
/// caller's transaction contains both the exposure and its inverse; normal
/// errors restore names before returning, and transaction rollback handles
/// unwinding or a failed inverse rename.
pub(super) struct RowIdentityExposure<'connection> {
    connection: &'connection Connection,
    renamed: Vec<RenamedColumn>,
}

struct RenamedColumn {
    table: String,
    original: String,
    exposed: String,
}

impl<'connection> RowIdentityExposure<'connection> {
    pub(super) fn new(connection: &'connection Connection) -> Self {
        Self {
            connection,
            renamed: Vec::new(),
        }
    }

    pub(super) fn expose(
        &mut self,
        table: &str,
        columns: &mut [String],
    ) -> Result<&'static str, DbError> {
        if let Some(alias) = ["rowid", "_rowid_", "oid"].into_iter().find(|alias| {
            !columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(alias))
        }) {
            return Ok(alias);
        }
        let index = columns
            .iter()
            .position(|column| column.eq_ignore_ascii_case("rowid"))
            .expect("all physical rowid aliases are shadowed");
        let mut suffix = 0usize;
        let exposed = loop {
            let candidate = format!("coven_projection_rowid_{suffix}");
            if !columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(&candidate))
            {
                break candidate;
            }
            suffix += 1;
        };
        let original = columns[index].clone();
        self.connection.execute_batch(&format!(
            "ALTER TABLE main.{} RENAME COLUMN {} TO {}",
            crate::quote_ident(table),
            crate::quote_ident(&original),
            exposed,
        ))?;
        self.renamed.push(RenamedColumn {
            table: table.into(),
            original,
            exposed: exposed.clone(),
        });
        columns[index] = exposed;
        Ok("rowid")
    }

    pub(super) fn translate_edge(&self, child: &str, edge: &mut ForeignKeyEdge) {
        for renamed in &self.renamed {
            for columns in &mut edge.columns {
                if child.eq_ignore_ascii_case(&renamed.table)
                    && columns.child.eq_ignore_ascii_case(&renamed.original)
                {
                    columns.child = renamed.exposed.clone();
                }
                if edge.parent_table.eq_ignore_ascii_case(&renamed.table)
                    && columns.parent.eq_ignore_ascii_case(&renamed.original)
                {
                    columns.parent = renamed.exposed.clone();
                }
            }
        }
    }

    pub(super) fn run<T>(
        mut self,
        operation: impl FnOnce(&mut Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let result = operation(&mut self);
        let mut restoration = Ok(());
        for renamed in self.renamed.iter().rev() {
            // The original name is exactly a rowid alias, so it is an
            // identifier without quoting. SQLite restores dependent SQL and
            // may canonicalize its original bracket or backtick spelling.
            if let Err(error) = self.connection.execute_batch(&format!(
                "ALTER TABLE main.{} RENAME COLUMN {} TO {}",
                crate::quote_ident(&renamed.table),
                crate::quote_ident(&renamed.exposed),
                renamed.original,
            )) {
                restoration = Err(match restoration {
                    Ok(()) => DbError::from(error),
                    Err(previous) => DbError::context(
                        format!("another local row identity could not be restored: {error}"),
                        previous,
                    ),
                });
            }
        }
        match (result, restoration) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(restoration)) => Err(DbError::context(
                format!("restoring local row identity also failed: {restoration}"),
                error,
            )),
        }
    }
}
