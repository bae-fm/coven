//! Authenticated identities of the data a row change replaced (D5).

use crate::error::{require, Error, Rule};
use crate::value::{name, positive, row};
use crate::wire::wire_struct;
use coven_merge::{RowId, WriteId, WritePast};
use std::collections::BTreeMap;

/// One original row change, independent of its received interpretation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChangeId {
    /// The write containing the source change.
    pub write: WriteId,
    /// Its original table, key and audience, before any conversion.
    pub row: RowId,
}
wire_struct!(ChangeId, write, row);

/// One cell as named in the original setter's authenticated record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellSource {
    /// The original change that set the cell.
    pub change: ChangeId,
    /// The column's name in that change, before renaming.
    pub column: String,
}
wire_struct!(CellSource, change, column => crate::wire::get_name);

/// The author's observed generation source and replaced cell setters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteSources {
    /// Absent exactly at generation zero.
    pub generation: Option<ChangeId>,
    /// Authored column order is increasing name order. A delete retains every
    /// replaced setter even when its old scalar map is empty. Columns which
    /// have never been set have no setter; inserts replace no cells.
    pub setters: BTreeMap<String, CellSource>,
}
wire_struct!(WriteSources, generation, setters => crate::wire::get_name_map);

impl WriteSources {
    /// Generation first, then setters in authored column order. Repeated
    /// identities in different roles remain separate entries.
    pub fn changes(&self) -> impl Iterator<Item = &ChangeId> {
        self.generation
            .iter()
            .chain(self.setters.values().map(|s| &s.change))
    }

    pub(crate) fn validate(&self, generation: u64) -> Result<(), Error> {
        require(
            self.generation.is_none() == (generation == 0),
            "generation source",
            Rule::Generation,
        )?;
        for source in self.changes() {
            positive(source.write.number)?;
            row(&source.row)?;
        }
        for (column, source) in &self.setters {
            name(column)?;
            name(&source.column)?;
        }
        Ok(())
    }

    /// Source identities must have been read, including implicit own predecessors.
    /// Timestamps and original row/cell evidence are checked by the database.
    pub fn validate_past(&self, header: &crate::write::WriteHeader) -> Result<(), Error> {
        let past = header.had_read.causal_past(header.position);
        require(
            self.changes().all(|s| past.contains(&s.write)),
            "source read",
            Rule::Coverage,
        )
    }
}
