//! What a statement names only in a column nothing reads of a subquery, CTE
//! or view, which it doesn't depend on (SPEC.md section 8): the tables only
//! such a column reads, none of whose rows it depends on, and the columns of
//! its other tables that only such a column names.
//!
//! A column of a subquery, CTE or view that no clause names and no result
//! column left reads changes no row the statement returns: SQLite keeps no
//! row, orders none and groups none by it. So the tables of the SELECTs
//! inside it change nothing, and neither does a column that only it reads,
//! as each column of the subquery a `SELECT w.*` around it passes on. A
//! column the subquery's own rows can depend on is read: each one of a
//! SELECT that is DISTINCT, groups its rows or has an ORDER BY, or of a
//! compound but one of UNION ALLs alone, and one that calls an aggregate,
//! which makes one row of many, or a window function. The model states the
//! rule as `Statement.pruned` and proves it in `crates/sqlive/proofs`.
//!
//! A view or a recursive CTE read whole in such a column counts for nothing
//! either, nor do the reads SQLite reports inside it, which name the view or
//! CTE, or a view or CTE inside it, unless a view or CTE the statement reads
//! whole elsewhere can report a read by the same name.

use std::collections::{BTreeMap, BTreeSet};

use crate::derived::Output;
use crate::from_item::Columns;
use crate::read_set::ColumnSet;
use crate::scope_plan::{Object, Planner, Source, Subqueries};
use crate::select_syntax::{parts, Part};
use sqlite3_parser::ast::Expr;

/// What a statement reads only in columns nothing reads.
#[derive(Default)]
pub(crate) struct UnreadReads {
    /// The tables and views read whole whose every source sits in such a
    /// column, lowercase.
    sources: BTreeSet<String>,
    /// The names, lowercase, only a view read whole in such a column can
    /// report a read through.
    inside: BTreeSet<String>,
}

impl UnreadReads {
    /// The names only a view read whole in such a column can report a read
    /// through.
    pub(crate) fn inside(&self) -> impl Iterator<Item = &String> {
        self.inside.iter()
    }

    /// Whether the statement depends on no read of `table`, where it reads
    /// each other table in a scope of `live`, SQLite reported a read of
    /// `table` through each of `through` and, when `direct`, one through no
    /// view or CTE, and a read through one of `inlined` is one a source
    /// makes or one that gives the statement no row.
    pub(crate) fn covers<T>(
        &self,
        table: &str,
        live: &BTreeMap<String, T>,
        direct: bool,
        through: Option<&BTreeSet<String>>,
        inlined: &BTreeSet<String>,
    ) -> bool {
        if self.sources.contains(table) {
            return through.is_none_or(|names| names.is_subset(inlined));
        }
        // A table no source of the statement's reads, which only views read
        // whole read.
        !live.contains_key(table)
            && !direct
            && through.is_some_and(|names| names.is_subset(&self.inside))
    }
}

/// The columns of each table of `live` that only what nothing reads names:
/// those some source of the table names there, and no source of it reads.
pub(crate) fn unread_columns<T>(sources: &[Source], live: &BTreeMap<String, T>) -> ColumnSet {
    let mut found = ColumnSet::new();
    for table in live.keys() {
        let of_table = || sources.iter().filter(|source| source.table() == Some(table));
        let unread = of_table().flat_map(|source| &source.columns.unread);
        let unread =
            unread.filter(|column| !of_table().any(|source| source.columns.read.contains(*column)));
        found.extend(unread.map(|column| (table.clone(), column.clone())));
    }
    found
}

/// What `sources` read only in columns nothing reads, where `live` are the
/// tables the other sources read.
pub(crate) fn unread_reads<T>(sources: &[Source], live: &BTreeMap<String, T>) -> UnreadReads {
    // The views read whole the statement reads outside such columns, and the
    // names reads inside them can come with, unless the parser can't tell
    // those of one.
    let mut live_views = BTreeSet::new();
    let mut live_inside = Some(BTreeSet::new());
    for source in sources.iter().filter(|source| !source.unread) {
        let inside = match &source.object {
            Object::View { name, inside, .. } => {
                live_views.insert(name.as_str());
                inside
            }
            Object::Recursive { inside, .. } => inside,
            Object::Table { .. } | Object::Function | Object::Derived => continue,
        };
        match (inside, &mut live_inside) {
            (Some(inside), Some(names)) => names.extend(inside.iter().cloned()),
            _ => live_inside = None,
        }
    }
    let mut found = UnreadReads::default();
    for source in sources.iter().filter(|source| source.unread) {
        match &source.object {
            Object::Table { name, .. } if !live.contains_key(name) => {
                found.sources.insert(name.clone());
            }
            Object::View { name, inside, .. } => {
                let Some(live_inside) = &live_inside else { continue };
                if !live_views.contains(name.as_str()) && !live_inside.contains(name) {
                    found.sources.insert(name.clone());
                }
                let inside = inside.iter().flatten().filter(|name| !live_inside.contains(*name));
                found.inside.extend(inside.cloned());
            }
            Object::Recursive { inside, .. } => {
                let Some(live_inside) = &live_inside else { continue };
                let inside = inside.iter().flatten().filter(|name| !live_inside.contains(*name));
                found.inside.extend(inside.cloned());
            }
            Object::Table { .. } | Object::Function | Object::Derived => {}
        }
    }
    found
}

/// A column a clause or a result column names.
pub(crate) enum ColumnRef {
    /// The column, lowercase, of the item at this position.
    Of(usize, String),
    /// A name, lowercase, sqlive resolves to no one item's column: it may
    /// name any column of that name.
    Named(String),
}

/// Which of a statement's cores and result columns it reads.
pub(crate) struct Live {
    /// Whether each core sits inside a column nothing reads.
    dead: Vec<bool>,
    /// The columns nothing reads, each as its core and its position.
    unread: BTreeSet<(usize, usize)>,
}

/// What one result column of a core reads.
pub(crate) struct OutputRead {
    refs: Vec<ColumnRef>,
    /// The cores of the SELECTs inside it.
    cores: Vec<usize>,
    /// Whether it calls no aggregate and no window function, so the core's
    /// rows are the same without it.
    keeps_rows: bool,
}

impl Planner<'_, '_> {
    /// The columns the names among `found`, written in `core`, name.
    pub(crate) fn column_refs(&self, core: usize, found: &[Part<'_>]) -> Vec<ColumnRef> {
        found
            .iter()
            .filter_map(|part| match part {
                Part::Name(name) => Some(self.column_ref(core, name)),
                _ => None,
            })
            .collect()
    }

    fn column_ref(&self, core: usize, name: &Expr<'_>) -> ColumnRef {
        match self.resolve(core, name) {
            Some((item, column)) => ColumnRef::Of(item, column),
            None => {
                let name = crate::sql::column_name(name).expect("a name part is a name");
                ColumnRef::Named(crate::sql::name(name).to_ascii_lowercase())
            }
        }
    }

    /// What each of `outputs`, the result columns of `core`, reads, where
    /// `subqueries` are the SELECTs read in the core's clauses.
    pub(crate) fn output_reads(
        &self,
        core: usize,
        outputs: &[Output<'_>],
        subqueries: &Subqueries<'_>,
    ) -> Vec<OutputRead> {
        outputs
            .iter()
            .map(|output| {
                let Some(expr) = output.written() else {
                    // A `*`'s column reads the one it passes on.
                    let refs = output.passed().map(|(item, column)| ColumnRef::Of(item, column));
                    return OutputRead {
                        refs: refs.into_iter().collect(),
                        cores: Vec::new(),
                        keeps_rows: true,
                    };
                };
                let mut found = Vec::new();
                parts(expr, &mut found);
                let cores = found
                    .iter()
                    .filter_map(|part| match part {
                        Part::Select(select) | Part::Subquery(select) => {
                            subqueries.iter().find(|(read, _)| std::ptr::eq(*read, *select))
                        }
                        _ => None,
                    })
                    .flat_map(|(_, cores)| cores.iter().copied())
                    .collect();
                OutputRead {
                    refs: self.column_refs(core, &found),
                    cores,
                    keeps_rows: !found
                        .iter()
                        .any(|part| part.aggregates(self.functions) || part.windowed()),
                }
            })
            .collect()
    }

    /// What the statement reads: every column and core but those of columns
    /// nothing reads.
    pub(crate) fn live(&self) -> Live {
        // The columns nothing reads, each as its core and its position, found
        // until no more are: leaving one out can leave another unnamed.
        let mut live = Live { dead: vec![false; self.cores.len()], unread: BTreeSet::new() };
        loop {
            live.dead = self.cores_inside(&live.unread);
            let (mut named, mut names) = (BTreeSet::new(), BTreeSet::new());
            for column in self.refs_read(&live) {
                match column {
                    ColumnRef::Of(item, column) => {
                        named.insert((*item, column.as_str()));
                    }
                    ColumnRef::Named(name) => {
                        names.insert(name.as_str());
                    }
                }
            }
            let mut next = BTreeSet::new();
            let cores = self.cores.iter().enumerate().filter(|(core, _)| !live.dead[*core]);
            for item in cores.flat_map(|(_, core)| core.items.iter().copied()) {
                let Some(derived) = &self.items[item].derived else { continue };
                let Columns::Listed(columns) = &self.items[item].columns else { continue };
                for &arm in derived.prunable_arms() {
                    for (position, column) in columns.iter().enumerate() {
                        // A column sqlive can't name, a `*` can still read.
                        let Some(name) = column.name.as_deref() else { continue };
                        let keeps_rows = self.cores[arm]
                            .outputs
                            .get(position)
                            .is_some_and(|output| output.keeps_rows);
                        if keeps_rows && !named.contains(&(item, name)) && !names.contains(name) {
                            next.insert((arm, position));
                        }
                    }
                }
            }
            if next == live.unread {
                return live;
            }
            live.unread = next;
        }
    }

    /// The columns the clauses and result columns of `live` name.
    pub(crate) fn refs_read<'a>(&'a self, live: &'a Live) -> impl Iterator<Item = &'a ColumnRef> {
        let cores = self.cores.iter().enumerate().filter(|(core, _)| !live.dead[*core]);
        cores.flat_map(move |(core, found)| {
            let outputs = found
                .outputs
                .iter()
                .enumerate()
                .filter(move |(position, _)| !live.unread.contains(&(core, *position)))
                .flat_map(|(_, output)| &output.refs);
            found.clause_refs.iter().chain(outputs)
        })
    }

    /// Every column the statement names, whether it reads it or not.
    pub(crate) fn every_ref(&self) -> impl Iterator<Item = &ColumnRef> {
        self.cores.iter().flat_map(|core| {
            let outputs = core.outputs.iter().flat_map(|output| &output.refs);
            core.clause_refs.iter().chain(outputs)
        })
    }

    /// The items that only a column nothing reads reads.
    pub(crate) fn unread_items(&self, live: &Live) -> Vec<usize> {
        (0..self.cores.len())
            .filter(|core| live.dead[*core])
            .flat_map(|core| self.cores[core].items.iter().copied())
            .collect()
    }

    /// Whether each core sits inside one of the columns `unread`: a SELECT in
    /// it, or one inside such a SELECT, those its FROM reads included.
    fn cores_inside(&self, unread: &BTreeSet<(usize, usize)>) -> Vec<bool> {
        let mut inside = vec![false; self.cores.len()];
        let mut found: Vec<usize> = unread
            .iter()
            .flat_map(|(core, position)| self.cores[*core].outputs[*position].cores.clone())
            .collect();
        while let Some(core) = found.pop() {
            if std::mem::replace(&mut inside[core], true) {
                continue;
            }
            found.extend(
                (0..self.cores.len()).filter(|child| self.cores[*child].parent() == Some(core)),
            );
            for &item in &self.cores[core].items {
                if let Some(derived) = &self.items[item].derived {
                    found.extend(derived.cores.iter().copied());
                }
            }
        }
        inside
    }
}
