//! The columns of each table a handle's live queries read, whose values a write
//! keeps of the rows it changes (SPEC.md section 2): those the queries read and
//! those their scopes test. The handle keeps them as it files each
//! subscription's reads again, so a write learns them without reading every
//! live query.

use crate::change_capture::KeptColumns;
use crate::ReadSet;
use std::collections::{BTreeMap, BTreeSet};

/// How many reads read each table, and how many of those read or test each
/// of its columns, all lowercase.
#[derive(Default)]
pub(crate) struct KeptCounts {
    tables: BTreeMap<String, TableCounts>,
}

#[derive(Default)]
struct TableCounts {
    reads: usize,
    columns: BTreeMap<String, usize>,
}

impl KeptCounts {
    /// Count the tables and columns of `reads`.
    pub(crate) fn add(&mut self, reads: &ReadSet) {
        for (table, columns) in kept(reads) {
            let counts = self.tables.entry(table).or_default();
            counts.reads += 1;
            for column in columns {
                *counts.columns.entry(column).or_default() += 1;
            }
        }
    }

    /// Take back what `add` counted for `reads`.
    pub(crate) fn remove(&mut self, reads: &ReadSet) {
        for (table, columns) in kept(reads) {
            let counts = self.tables.get_mut(&table).expect("`add` counted the read's table");
            counts.reads -= 1;
            for column in columns {
                let count =
                    counts.columns.get_mut(&column).expect("`add` counted the read's column");
                *count -= 1;
                if *count == 0 {
                    counts.columns.remove(&column);
                }
            }
            if counts.reads == 0 {
                self.tables.remove(&table);
            }
        }
    }

    /// The columns some read reads or tests, by table: every table some
    /// read reads, with no column where it keeps none.
    pub(crate) fn columns(&self) -> KeptColumns {
        self.tables
            .iter()
            .map(|(table, counts)| (table.clone(), counts.columns.keys().cloned().collect()))
            .collect()
    }
}

/// Each read of `reads` with its table, lowercase, and the columns it reads
/// or its scope tests.
fn kept(reads: &ReadSet) -> impl Iterator<Item = (String, BTreeSet<String>)> + '_ {
    reads.iter().map(|read| {
        // A view read whole names no column of its own.
        let mut columns: BTreeSet<String> =
            read.columns.iter().filter(|column| !column.is_empty()).cloned().collect();
        read.scope.columns(&mut columns);
        (read.table.to_ascii_lowercase(), columns)
    })
}
