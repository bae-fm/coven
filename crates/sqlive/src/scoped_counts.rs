//! The columns the scopes of a handle's live queries test, by table, which a
//! write records the values of in the rows it changes (§4). The handle keeps
//! them as it files each subscription's reads again, so a write learns them
//! without reading every live query.

use crate::change_capture::ScopedColumns;
use crate::read_set::ReadSet;
use crate::row_scope::RowScope;
use std::collections::{BTreeMap, BTreeSet};

/// How many reads' scopes test each column, by table, all lowercase.
#[derive(Default)]
pub(crate) struct ScopedCounts {
    tables: BTreeMap<String, BTreeMap<String, usize>>,
}

impl ScopedCounts {
    /// Count the columns the scopes of `reads` test.
    pub(crate) fn add(&mut self, reads: &ReadSet) {
        for (table, columns) in tested(reads) {
            let counts = self.tables.entry(table).or_default();
            for column in columns {
                *counts.entry(column).or_default() += 1;
            }
        }
    }

    /// Take back the columns `add` counted for `reads`.
    pub(crate) fn remove(&mut self, reads: &ReadSet) {
        for (table, columns) in tested(reads) {
            let counts = self.tables.get_mut(&table).expect("`add` counted the read's table");
            for column in columns {
                let count = counts.get_mut(&column).expect("`add` counted the read's column");
                *count -= 1;
                if *count == 0 {
                    counts.remove(&column);
                }
            }
            if counts.is_empty() {
                self.tables.remove(&table);
            }
        }
    }

    /// The columns some read's scope tests, by table.
    pub(crate) fn columns(&self) -> ScopedColumns {
        self.tables
            .iter()
            .map(|(table, counts)| (table.clone(), counts.keys().cloned().collect()))
            .collect()
    }
}

/// Each read of `reads` whose scope tests some column, with its table,
/// lowercase, and those columns.
fn tested(reads: &ReadSet) -> impl Iterator<Item = (String, BTreeSet<String>)> + '_ {
    reads
        .iter()
        .filter(|read| !matches!(read.scope, RowScope::All))
        .map(|read| {
            let mut columns = BTreeSet::new();
            read.scope.columns(&mut columns);
            (read.table.to_ascii_lowercase(), columns)
        })
        .filter(|(_, columns)| !columns.is_empty())
}
