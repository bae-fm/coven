//! Subqueries in FROM, CTEs and the app's views, read as cores of their
//! own, and the conditions on their rows passed into those cores (§4).
//!
//! A row of a subquery comes from rows of its sources. When the subquery
//! neither groups nor limits its rows, an output column that is a bare
//! column of a source holds that source row's value, so a condition the
//! statement puts on the column holds of the source rows too: a source row
//! failing it makes only output rows that fail it.

use crate::from_item::{Column, Columns, FromItem};
use crate::scope_plan::{is_name, read_select, unquoted, Edge, Planner, Source};
use crate::scope_term::ScopeTerm;
use crate::select_syntax::{parts, quoted, text, Part};
use rusqlite::Connection;
use sqlite3_parser::{
    ast::{As, Cmd, CommonTableExpr, Name, OneSelect, ResultColumn, Select, Stmt, With},
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

/// The CTEs a FROM item can name where it is read, and those whose SQL is
/// being read now, which naming again would recurse.
#[derive(Clone, Default)]
pub(crate) struct Ctes<'a> {
    visible: Vec<&'a CommonTableExpr<'a>>,
    reading: Vec<String>,
}

impl<'a> Ctes<'a> {
    /// These and `with`'s, or `None` when a name repeats, which SQLite
    /// resolves by where each is and coven doesn't follow, or names one of
    /// coven's views, which reads through the CTE couldn't be told from.
    pub(crate) fn with(&self, with: &'a With<'a>) -> Option<Self> {
        let mut visible = self.visible.clone();
        for cte in with.ctes.iter() {
            if visible.iter().any(|seen| seen.tbl_name == cte.tbl_name)
                || crate::internal_schema::app_readable(&unquoted(cte.tbl_name.0))
            {
                return None;
            }
            visible.push(cte);
        }
        Some(Self { visible, reading: self.reading.clone() })
    }

    /// The CTE `name` names.
    pub(crate) fn named(&self, name: &Name<'_>) -> Option<&'a CommonTableExpr<'a>> {
        self.visible.iter().copied().find(|cte| cte.tbl_name == *name)
    }

    /// Every CTE, as a WITH clause.
    fn clause(&self) -> String {
        let ctes: Vec<String> = self.visible.iter().map(|cte| text(*cte)).collect();
        format!("WITH {}", ctes.join(","))
    }
}

/// What a FROM item reads rows from, as SQL of its own.
pub(crate) enum Named<'a, 'v> {
    Subquery(&'a Select<'a>),
    Cte(&'a CommonTableExpr<'a>),
    /// One of the app's views, by its stored name.
    View(&'v str),
}

/// The cores of a subquery, CTE or view that a condition on its rows
/// narrows: for each such arm, the source and column each output column
/// passes on unchanged, if it does.
pub(crate) struct Derived {
    arms: Vec<Vec<Option<(usize, String)>>>,
}

/// Read `named` as a FROM item of a core that sits in `parent`: its source,
/// its columns and its cores, or `None` when coven can't read its rows.
pub(crate) fn read_derived_item<'a>(
    connection: &Connection,
    planner: &mut Planner,
    named: Named<'a, '_>,
    qualifier: String,
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Option<(Source, Columns, Derived)>> {
    let found = match named {
        Named::Subquery(select) => {
            // A subquery reads the names around its core, not its core's.
            let from = parent.is_none().then(|| {
                if ctes.visible.is_empty() {
                    format!("({}) AS {qualifier}", text(select))
                } else {
                    format!("({} SELECT * FROM ({})) AS {qualifier}", ctes.clause(), text(select))
                }
            });
            read_derived(connection, planner, select, None, parent, ctes)?
                .map(|(columns, derived)| (from, columns, derived))
        }
        Named::Cte(cte) => {
            let name = unquoted(cte.tbl_name.0).to_ascii_lowercase();
            // A CTE naming itself, or one naming it, recurses.
            if ctes.reading.contains(&name) {
                return Ok(None);
            }
            let from = parent.is_none().then(|| {
                format!("({} SELECT * FROM {}) AS {qualifier}", ctes.clause(), quoted(&name))
            });
            let mut inner = ctes.clone();
            inner.reading.push(name.clone());
            planner.inlined.push(name);
            let names = cte.columns.map(|columns| {
                columns
                    .iter()
                    .map(|column| unquoted(column.col_name.0).to_ascii_lowercase())
                    .collect()
            });
            read_derived(connection, planner, cte.select, names, parent, &inner)?
                .map(|(columns, derived)| (from, columns, derived))
        }
        Named::View(stored) => {
            let sql: String = connection.query_row(
                "SELECT sql FROM main.sqlite_schema WHERE type='view' AND name=?1",
                [stored],
                |row| row.get(0),
            )?;
            let bump = Bump::new();
            let mut parser = Parser::new(&bump, sql.as_bytes());
            let Ok(Some(Cmd::Stmt(Stmt::CreateView { columns, select, .. }))) = parser.next()
            else {
                return Ok(None);
            };
            let names = columns.map(|columns| {
                columns
                    .iter()
                    .map(|column| unquoted(column.col_name.0).to_ascii_lowercase())
                    .collect()
            });
            // A view names only the schema's tables and views.
            read_derived(connection, planner, select, names, None, &Ctes::default())?.map(
                |(columns, derived)| {
                    let from = Some(format!("main.{} AS {qualifier}", quoted(stored)));
                    (from, columns, derived)
                },
            )
        }
    };
    Ok(found.map(|(from, columns, derived)| {
        (
            Source { table: None, from, qualifier, term: None, memberships: Vec::new() },
            columns,
            derived,
        )
    }))
}

/// Read `select`'s cores, sitting in `parent`, and name its columns
/// `names`, or as SQLite names its first arm's.
fn read_derived<'a>(
    connection: &Connection,
    planner: &mut Planner,
    select: &'a Select<'a>,
    names: Option<Vec<String>>,
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Option<(Columns, Derived)>> {
    let Some(cores) = read_select(connection, planner, select, parent, ctes)? else {
        return Ok(None);
    };
    let bodies = std::iter::once(&select.body.select)
        .chain(select.body.compounds.iter().flatten().map(|compound| &compound.select));
    let mut arms = Vec::new();
    for (core, body) in cores.iter().copied().zip(bodies) {
        let Some(columns) = arm_columns(planner, core, body) else {
            // A `*` over a table function's columns: no column is known.
            return Ok(Some((Columns::Unknown, Derived { arms: Vec::new() })));
        };
        let grouped = match body {
            OneSelect::Select { group_by, having, window_clause, .. } => {
                group_by.is_some() || having.is_some() || window_clause.is_some()
            }
            OneSelect::Values(_) => false,
        };
        arms.push((core, columns, grouped || aggregates(planner, select, body)));
    }
    let width = arms[0].1.len();
    let named: Vec<Option<String>> = match names {
        Some(names) if names.len() == width => names.into_iter().map(Some).collect(),
        Some(_) => return Ok(None),
        None => arms[0].1.iter().map(|output| output.name.clone()).collect(),
    };
    let mut columns = Vec::new();
    for (position, name) in named.into_iter().enumerate() {
        // SQLite tells a repeated name apart by a suffix of its own.
        let name = name.filter(|name| {
            !columns.iter().any(|column: &Column| column.name.as_ref() == Some(name))
        });
        // The column compares as the columns it passes on do in every arm.
        let mut affinities = arms.iter().map(|(_, columns, _)| {
            let (item, column) = columns.get(position)?.passed.as_ref()?;
            planner.items[*item].columns.affinity(column)
        });
        let first = affinities.next().flatten();
        let affinity = first.filter(|first| affinities.all(|other| other == Some(*first)));
        columns.push(Column { name, affinity });
    }
    // A LIMIT keeps rows by the others around them, and grouping makes one
    // row of many: a condition on such a row says nothing of its sources.
    let arms = if select.limit.is_some() {
        Vec::new()
    } else {
        arms.into_iter()
            .filter(|(_, _, grouped)| !grouped)
            .map(|(_, columns, _)| columns.into_iter().map(|output| output.passed).collect())
            .collect()
    };
    Ok(Some((Columns::Listed(columns), Derived { arms })))
}

/// Whether `body` calls an aggregate or window function, which makes its
/// rows of many of its sources' rows.
fn aggregates(planner: &Planner, select: &Select<'_>, body: &OneSelect<'_>) -> bool {
    let mut found = Vec::new();
    if let OneSelect::Select { columns, .. } = body {
        for column in columns.iter() {
            if let ResultColumn::Expr(expr, _) = column {
                parts(expr, &mut found);
            }
        }
    }
    if select.body.compounds.is_none() {
        for term in select.order_by.iter().flat_map(|terms| terms.iter()) {
            parts(&term.expr, &mut found);
        }
    }
    found.iter().any(|part| match part {
        Part::Function(name, window) => {
            *window || planner.aggregates.contains(&name.to_ascii_lowercase())
        }
        _ => false,
    })
}

/// A column a core outputs: its name as SQLite gives it, when coven knows
/// it, and the source of the core's own and the column of it the output
/// passes on unchanged, if it does.
struct Output {
    name: Option<String>,
    passed: Option<(usize, String)>,
}

/// Each output column of the arm `core`, read from `body`, or `None` when a
/// `*` spans columns coven doesn't know.
fn arm_columns(planner: &Planner, core: usize, body: &OneSelect<'_>) -> Option<Vec<Output>> {
    let OneSelect::Select { columns, .. } = body else {
        let OneSelect::Values(rows) = body else { unreachable!("a core is a SELECT or VALUES") };
        // VALUES names its columns column1, column2, …
        return Some(
            (1..=rows.first().map_or(0, |row| row.len()))
                .map(|position| Output { name: Some(format!("column{position}")), passed: None })
                .collect(),
        );
    };
    let mut found = Vec::new();
    for column in columns.iter() {
        match column {
            ResultColumn::Expr(expr, alias) => {
                let passed = is_name(expr)
                    .then(|| planner.resolve(core, expr))
                    .flatten()
                    .filter(|(item, _)| planner.items[*item].core == core);
                let name = match alias {
                    Some(As::As(alias) | As::Elided(alias)) => {
                        Some(unquoted(alias.0).to_ascii_lowercase())
                    }
                    None => passed.as_ref().map(|(_, column)| column.clone()),
                };
                found.push(Output { name, passed });
            }
            ResultColumn::Star => {
                for item in &planner.cores[core].items {
                    found.extend(every_column(&planner.items[*item], *item)?);
                }
            }
            ResultColumn::TableStar(qualifier) => {
                let item = planner.cores[core]
                    .items
                    .iter()
                    .copied()
                    .find(|item| *qualifier == Name(&planner.items[*item].source.qualifier))?;
                found.extend(every_column(&planner.items[item], item)?);
            }
        }
    }
    Some(found)
}

/// The columns a `*` gives of `item`, at position `position`.
fn every_column(item: &FromItem, position: usize) -> Option<Vec<Output>> {
    match &item.columns {
        Columns::Listed(columns) => Some(
            columns
                .iter()
                .map(|column| Output {
                    name: column.name.clone(),
                    passed: column.name.clone().map(|name| (position, name)),
                })
                .collect(),
        ),
        Columns::Unknown => None,
    }
}

/// Narrow the cores of the subquery, CTE or view `item` by `term` and
/// `edges`, which its own core's conditions put on its rows: each arm's
/// source an output column passes on takes them as its own, unless a LEFT
/// JOIN adds it there.
pub(crate) fn push_down(
    planner: &mut Planner,
    item: usize,
    term: Option<ScopeTerm>,
    edges: Vec<Edge>,
) {
    let Some(derived) = &planner.items[item].derived else {
        return;
    };
    if term.is_none() && edges.is_empty() {
        return;
    }
    let Columns::Listed(columns) = &planner.items[item].columns else {
        return;
    };
    let positions: Vec<Option<String>> = columns.iter().map(|column| column.name.clone()).collect();
    for arm in derived.arms.clone() {
        let through = |column: &str| {
            let position = positions.iter().position(|name| name.as_deref() == Some(column))?;
            arm[position].clone()
        };
        let mut targets: Vec<usize> = Vec::new();
        let mut tested = Vec::new();
        if let Some(term) = &term {
            term.columns(&mut tested);
        }
        let reached =
            tested.iter().map(String::as_str).chain(edges.iter().map(|edge| edge.column.as_str()));
        for column in reached {
            if let Some((target, _)) = through(column) {
                if !targets.contains(&target) {
                    targets.push(target);
                }
            }
        }
        for target in targets {
            let found = &planner.items[target];
            if found.nullable || (found.source.table.is_none() && found.derived.is_none()) {
                continue;
            }
            let renamed = term.as_ref().and_then(|term| {
                term.renamed(&|column| {
                    through(column)
                        .filter(|(passed, _)| *passed == target)
                        .map(|(_, column)| column)
                })
            });
            let passed: Vec<Edge> = edges
                .iter()
                .filter_map(|edge| {
                    let (passed, column) = through(&edge.column)?;
                    (passed == target).then(|| Edge { member: target, column, ..edge.clone() })
                })
                .collect();
            if let Some(renamed) = &renamed {
                let source = &mut planner.items[target].source;
                source.term = Some(match source.term.take() {
                    Some(existing) => existing.and(renamed.clone()),
                    None => renamed.clone(),
                });
            }
            planner.edges.extend(passed.iter().cloned());
            push_down(planner, target, renamed, passed);
        }
    }
}

#[cfg(test)]
#[path = "derived_tests.rs"]
mod tests;
