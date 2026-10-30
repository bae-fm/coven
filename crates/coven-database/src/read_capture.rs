//! What the statements a live query runs read, as the authorizer reports
//! it while each is prepared, and the scope each table was read in (§4).

use std::collections::{BTreeMap, BTreeSet};

use crate::observation::{ColumnSet, ReadSet, TableRead};
use crate::row_scope::{RowScope, Scopes};

/// What a read context's statements read. Each statement running has its own
/// frame, innermost last, so a statement a row mapper starts records its own
/// reads and leaves the statement around it with its own. The first frame
/// holds reads made outside any statement.
pub(crate) struct ReadCapture {
    frames: Vec<ReadFrame>,
    completed: ReadSet,
}

/// What one statement read, as SQLite reported it while preparing it.
#[derive(Clone, Default)]
pub(crate) struct ReadFrame {
    columns: ColumnSet,
    /// Tables some of whose columns it read through a view or a CTE, and
    /// the names, lowercase, of the innermost each read was through.
    indirect: BTreeMap<String, BTreeSet<String>>,
}

impl ReadFrame {
    fn extend(&mut self, other: &Self) {
        self.columns.extend(other.columns.iter().cloned());
        for (table, views) in &other.indirect {
            self.indirect
                .entry(table.clone())
                .or_default()
                .extend(views.iter().cloned());
        }
    }
}

impl ReadCapture {
    pub(crate) fn new() -> Self {
        Self {
            frames: vec![ReadFrame::default()],
            completed: ReadSet::new(),
        }
    }

    fn current(&mut self) -> &mut ReadFrame {
        self.frames
            .last_mut()
            .expect("a read capture keeps its first frame")
    }

    /// The statement being prepared read `column` of `table`, through the
    /// view or CTE `accessor` when it names one.
    pub(crate) fn read(&mut self, table: &str, column: &str, accessor: Option<&str>) {
        let frame = self.current();
        frame
            .columns
            .insert((table.to_ascii_lowercase(), column.to_ascii_lowercase()));
        if let Some(view) = accessor {
            frame
                .indirect
                .entry(table.to_ascii_lowercase())
                .or_default()
                .insert(view.to_ascii_lowercase());
        }
        // A read through one of coven's views reads the view itself too,
        // even when the statement reads none of the view's columns, as a
        // count of its rows or an EXISTS doesn't: a view can change with no
        // change to its tables, as `coven_sending` does (§4).
        if let Some(view) = accessor.filter(|name| crate::internal_schema::app_readable(name)) {
            frame
                .columns
                .insert((view.to_ascii_lowercase(), String::new()));
        }
    }

    /// What the statement being prepared has read so far.
    pub(crate) fn statement(&mut self) -> ReadFrame {
        self.current().clone()
    }

    /// The statement being run read what `recorded` holds when it was
    /// prepared.
    pub(crate) fn replay(&mut self, recorded: &ReadFrame) {
        self.current().extend(recorded);
    }

    /// A statement begins.
    pub(crate) fn begin(&mut self) {
        self.frames.push(ReadFrame::default());
    }

    /// End the statement begun last: each table it read in the scope
    /// `scopes` gives it, or whole.
    pub(crate) fn end(&mut self, scopes: &Scopes) {
        assert!(self.frames.len() > 1, "a statement's frame was begun");
        let frame = self.frames.pop().expect("the statement's frame");
        self.finish(frame, scopes);
    }

    /// Everything read so far, each statement still running counted whole.
    pub(crate) fn reads(&mut self) -> ReadSet {
        for frame in std::mem::take(&mut self.frames) {
            self.finish(frame, &Scopes::default());
        }
        self.frames.push(ReadFrame::default());
        self.completed.clone()
    }

    /// Record `frame`'s tables, each read through the scope `scopes` gives
    /// it, or whole: a table read through a view or CTE the statement didn't
    /// read as a source of its own, as one of coven's views, can be read in
    /// any row.
    fn finish(&mut self, frame: ReadFrame, scopes: &Scopes) {
        let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (table, column) in frame.columns {
            tables.entry(table).or_default().insert(column);
        }
        self.completed
            .extend(tables.into_iter().map(|(table, columns)| {
                TableRead {
                    scope: match scopes.tables.get(&table) {
                        Some(scope)
                            if frame
                                .indirect
                                .get(&table)
                                .is_none_or(|views| views.is_subset(&scopes.inlined)) =>
                        {
                            scope.clone()
                        }
                        _ => RowScope::All,
                    },
                    table,
                    columns,
                    inserted_only: false,
                }
            }));
    }
}
