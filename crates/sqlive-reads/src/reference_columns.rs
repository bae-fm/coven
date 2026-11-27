//! The columns a statement reads through each reference to a table, each FROM
//! item that names one (SPEC.md section 4): those its clauses name of the
//! item, and every column of an item whose columns this can't tell apart: one
//! in a query whose rows SQLite picks, or in a core whose rows follow the
//! order SQLite reads its rows in (`unreported_columns.rs`), one in a
//! parenthesized join that SQLite reads through a `SELECT *` of its own, and
//! one a `*` over columns sqlive doesn't know can span. The model states the
//! rule as `Statement.split` in `crates/sqlive/proofs`.
//!
//! A column that only what nothing reads names, as a view's result column the
//! statement doesn't select (`unread_columns.rs`), is read through no
//! reference. The model states that as `Statement.pruned`.

use std::collections::BTreeSet;

use sqlite3_parser::ast::Name;

use crate::schema_objects::ROWID_NAMES;
use crate::scope_plan::Planner;
use crate::table_keys::rowid_column;
use crate::unread_columns::{ColumnRef, Live};
use crate::unreported_columns::Node;

/// The columns, lowercase, one item names of the table it reads; none for an
/// item that reads no table.
pub(crate) struct ReferenceColumns {
    /// Those the statement reads through it.
    pub(crate) read: BTreeSet<String>,
    /// Those only what nothing reads names, which the statement doesn't read
    /// through it though SQLite reports them.
    pub(crate) unread: BTreeSet<String>,
}

impl Planner<'_, '_> {
    /// The columns each item names, in the order of the items. `live` says
    /// which clauses the statement reads, and `picked` which of its SELECTs
    /// and cores keep rows SQLite picks.
    pub(crate) fn reference_columns(
        &self,
        live: &Live,
        picked: &BTreeSet<Node>,
    ) -> rusqlite::Result<Vec<ReferenceColumns>> {
        let mut columns = self.named_columns(self.refs_read(live))?;
        for item in self.items_read_whole_rows(picked) {
            let Some(table) = self.items[item].source.table() else { continue };
            for column in self.schema.columns(table)? {
                columns[item].insert(column.name.to_ascii_lowercase());
            }
        }
        let named = self.named_columns(self.every_ref())?;
        Ok(columns
            .into_iter()
            .zip(named)
            .map(|(read, named)| {
                let unread = named.difference(&read).cloned().collect();
                ReferenceColumns { read, unread }
            })
            .collect())
    }

    /// The columns, lowercase, of each item's table that `refs` name, in the
    /// order of the items.
    fn named_columns<'r>(
        &self,
        refs: impl Iterator<Item = &'r ColumnRef>,
    ) -> rusqlite::Result<Vec<BTreeSet<String>>> {
        let mut columns = vec![BTreeSet::new(); self.items.len()];
        // The names that resolve to no one item's column.
        let mut unresolved = BTreeSet::new();
        for column in refs {
            match column {
                ColumnRef::Of(item, column) => {
                    columns[*item].insert(column.clone());
                }
                ColumnRef::Named(name) => {
                    unresolved.insert(name.as_str());
                }
            }
        }
        // Each such name can name that column of any table, and a rowid's
        // name the rowid of any.
        for (item, found) in self.items.iter().enumerate() {
            let Some(table) = found.source.table() else { continue };
            for &name in &unresolved {
                if let Some(column) = found.columns.named(&Name(name)) {
                    columns[item].insert(column);
                } else if ROWID_NAMES.contains(&name) {
                    // SQLite reports a read of the rowid as one of the
                    // column that holds it, or of `ROWID`.
                    let column = rowid_column(self.schema, table)?.unwrap_or("rowid");
                    columns[item].insert(column.to_ascii_lowercase());
                }
            }
        }
        Ok(columns)
    }

    /// The items every column of whose rows the statement reads.
    fn items_read_whole_rows(&self, picked: &BTreeSet<Node>) -> BTreeSet<usize> {
        let mut found: BTreeSet<usize> =
            (0..self.items.len()).filter(|item| self.items[*item].grouped).collect();
        for core in self.cores.iter().filter(|core| core.unknown_star) {
            found.extend(core.items.iter().copied());
        }
        // A query whose rows SQLite picks reads every column of each table
        // its cores read, and those of the subqueries, CTEs and views they
        // read.
        let mut waiting: Vec<usize> = self
            .selects
            .iter()
            .filter(|read| picked.contains(&read.node))
            .flat_map(|read| read.cores.iter().copied())
            .collect();
        waiting
            .extend((0..self.cores.len()).filter(|core| picked.contains(&self.cores[*core].node)));
        let mut read = BTreeSet::new();
        while let Some(core) = waiting.pop() {
            if !read.insert(core) {
                continue;
            }
            waiting.extend(
                (0..self.cores.len()).filter(|child| self.cores[*child].parent() == Some(core)),
            );
            for &item in &self.cores[core].items {
                found.insert(item);
                if let Some(derived) = &self.items[item].derived {
                    waiting.extend(derived.cores.iter().copied());
                }
            }
        }
        found
    }
}
