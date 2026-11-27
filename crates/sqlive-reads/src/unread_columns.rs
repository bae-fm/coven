//! The tables of a statement that only a column nothing reads of a subquery,
//! CTE or view reads, which the statement depends on no row of (SPEC.md
//! section 8).
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
//! A view read whole in such a column counts for nothing either, nor do the
//! reads SQLite reports inside it, which name the view, or a view or CTE
//! inside it, unless a view the statement reads whole elsewhere can report a
//! read by the same name.

use std::collections::{BTreeMap, BTreeSet};

use crate::derived::Output;
use crate::from_item::Columns;
use crate::row_scope::RowScope;
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
    pub(crate) fn covers(
        &self,
        table: &str,
        live: &BTreeMap<String, RowScope>,
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

/// What `sources` read only in columns nothing reads, where `live` are the
/// tables the other sources read.
pub(crate) fn unread_reads(sources: &[Source], live: &BTreeMap<String, RowScope>) -> UnreadReads {
    // The views read whole the statement reads outside such columns, and the
    // names reads inside them can come with, unless the parser can't tell
    // those of one.
    let mut live_views = BTreeSet::new();
    let mut live_inside = Some(BTreeSet::new());
    for source in sources.iter().filter(|source| !source.unread) {
        let Object::View { name, inside, .. } = &source.object else { continue };
        live_views.insert(name.as_str());
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

/// What one result column of a core reads.
pub(crate) struct OutputRead {
    refs: Vec<ColumnRef>,
    /// The cores of the SELECTs inside it.
    cores: Vec<usize>,
    /// Whether it calls no aggregate and no window function, so the core's
    /// rows are the same without it.
    keeps_rows: bool,
}

impl Planner<'_> {
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

    /// The items that only a column nothing reads reads.
    pub(crate) fn unread_items(&self) -> Vec<usize> {
        // The columns nothing reads, each as its core and its position, found
        // until no more are: leaving one out can leave another unnamed.
        let mut unread: BTreeSet<(usize, usize)> = BTreeSet::new();
        loop {
            let dead = self.cores_inside(&unread);
            let (mut named, mut names) = (BTreeSet::new(), BTreeSet::new());
            for (core, found) in self.cores.iter().enumerate().filter(|(core, _)| !dead[*core]) {
                let outputs = found
                    .outputs
                    .iter()
                    .enumerate()
                    .filter(|(position, _)| !unread.contains(&(core, *position)))
                    .flat_map(|(_, output)| &output.refs);
                for column in found.clause_refs.iter().chain(outputs) {
                    match column {
                        ColumnRef::Of(item, column) => {
                            named.insert((*item, column.as_str()));
                        }
                        ColumnRef::Named(name) => {
                            names.insert(name.as_str());
                        }
                    }
                }
            }
            let mut next = BTreeSet::new();
            let live = self.cores.iter().enumerate().filter(|(core, _)| !dead[*core]);
            for item in live.flat_map(|(_, core)| core.items.iter().copied()) {
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
            if next == unread {
                return (0..self.cores.len())
                    .filter(|core| dead[*core])
                    .flat_map(|core| self.cores[core].items.iter().copied())
                    .collect();
            }
            unread = next;
        }
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
