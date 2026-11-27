//! What the statements a live query runs read, as the authorizer reports it
//! while each is prepared, and the scope each table was read in
//! (SPEC.md section 4).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::read_set::{ColumnSet, ReadSet, TableRead};
use crate::row_scope::{RowScope, Scopes};

/// The views and tables a live query reads whole: a read through one of them
/// depends on the object itself, which can change with no change to the
/// tables it reads.
pub struct WholeViews(BTreeSet<String>);

impl WholeViews {
    /// The views and tables named `views`, in any letter case.
    pub fn new<'a>(views: impl IntoIterator<Item = &'a str>) -> Self {
        Self(views.into_iter().map(str::to_ascii_lowercase).collect())
    }

    /// Whether `name` names one, in any letter case.
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains(&name.to_ascii_lowercase())
    }
}

/// What a read context's statements read. Each statement running has its own
/// frame, innermost last, so a statement a row mapper starts records its own
/// reads and leaves the statement around it with its own. The first frame
/// holds reads made outside any statement.
pub struct ReadCapture {
    frames: Vec<ReadFrame>,
    completed: ReadSet,
    whole: Arc<WholeViews>,
}

/// What one statement read, as SQLite reported it while preparing it.
#[derive(Clone, Default)]
pub struct ReadFrame {
    columns: ColumnSet,
    /// Tables some of whose columns it read through a view or a CTE, and
    /// the names, lowercase, of the innermost each read was through.
    indirect: BTreeMap<String, BTreeSet<String>>,
}

impl ReadFrame {
    /// Whether each read of `table` through a view or CTE was through one of
    /// `inlined`.
    fn read_inside(&self, table: &str, inlined: &BTreeSet<String>) -> bool {
        self.indirect.get(table).is_none_or(|views| views.is_subset(inlined))
    }

    fn extend(&mut self, other: &Self) {
        self.columns.extend(other.columns.iter().cloned());
        for (table, views) in &other.indirect {
            self.indirect.entry(table.clone()).or_default().extend(views.iter().cloned());
        }
    }
}

impl ReadCapture {
    /// A capture with one frame, for reads made outside any statement.
    pub fn new(whole: Arc<WholeViews>) -> Self {
        Self { frames: vec![ReadFrame::default()], completed: ReadSet::new(), whole }
    }

    fn current(&mut self) -> &mut ReadFrame {
        self.frames.last_mut().expect("a read capture keeps its first frame")
    }

    /// The statement being prepared read `column` of `table`, through the
    /// view or CTE `accessor` when it names one.
    pub fn read(&mut self, table: &str, column: &str, accessor: Option<&str>) {
        let whole = accessor.filter(|name| self.whole.contains(name));
        let frame = self.current();
        frame.columns.insert((table.to_ascii_lowercase(), column.to_ascii_lowercase()));
        if let Some(view) = accessor {
            frame
                .indirect
                .entry(table.to_ascii_lowercase())
                .or_default()
                .insert(view.to_ascii_lowercase());
        }
        // A read through one of the views read whole reads the view itself
        // too, even when the statement reads none of the view's columns, as
        // a count of its rows or an EXISTS doesn't: a view can change with
        // no change to its tables, as one that calls a function of the
        // host's state can (SPEC.md section 3).
        if let Some(view) = whole {
            frame.columns.insert((view.to_ascii_lowercase(), String::new()));
        }
    }

    /// What the statement being prepared has read so far.
    pub fn statement(&mut self) -> ReadFrame {
        self.current().clone()
    }

    /// The statement being run read what `recorded` holds when it was
    /// prepared.
    pub fn replay(&mut self, recorded: &ReadFrame) {
        self.current().extend(recorded);
    }

    /// A statement begins.
    pub fn begin(&mut self) {
        self.frames.push(ReadFrame::default());
    }

    /// End the statement begun last: each table it read in the scope
    /// `scopes` gives it, or whole.
    pub fn end(&mut self, scopes: &Scopes) {
        assert!(self.frames.len() > 1, "a statement's frame was begun");
        let frame = self.frames.pop().expect("the statement's frame");
        self.finish(frame, scopes);
    }

    /// Everything read so far, each statement still running counted whole.
    pub fn reads(&mut self) -> ReadSet {
        for frame in std::mem::take(&mut self.frames) {
            self.finish(frame, &Scopes::default());
        }
        self.frames.push(ReadFrame::default());
        self.completed.clone()
    }

    /// Record `frame`'s tables but those `scopes` says count for nothing, the
    /// FTS5 shadow tables and the tables only an unread column reads,
    /// each read through the scope `scopes` gives it, or whole, with the
    /// columns its rows depend on that the authorizer
    /// didn't report (`unreported_columns.rs`): SQLite reports no read at all
    /// of a table that a subquery reads only through a USING join. A table
    /// read through a view or CTE that the
    /// statement didn't read as a source of its own, such as a view read
    /// whole, may give the statement any of its rows, so `finish` records it
    /// as read in every row.
    fn finish(&mut self, frame: ReadFrame, scopes: &Scopes) {
        let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let reported = frame.columns.iter().cloned();
        for (table, column) in reported
            .chain(scopes.unreported.columns().iter().cloned())
            .filter(|(table, _)| !scopes.fts5_shadows.contains(table))
            .filter(|(table, _)| {
                !scopes.unread.contains(table) || !frame.read_inside(table, &scopes.inlined)
            })
        {
            tables.entry(table).or_default().insert(column);
        }
        self.completed.extend(tables.into_iter().map(|(table, columns)| TableRead {
            scope: match scopes.tables.get(&table) {
                Some(scope) if frame.read_inside(&table, &scopes.inlined) => scope.clone(),
                _ => RowScope::All,
            },
            table,
            columns,
            inserted_only: false,
            host: scopes.host,
        }));
    }
}
