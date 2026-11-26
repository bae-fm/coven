//! Subqueries in FROM, CTEs and the app's views, read as cores of their own,
//! and the conditions on their rows passed into those cores
//! (SPEC.md section 8).
//!
//! A row of a subquery comes from rows of its sources. When the subquery
//! neither groups nor limits its rows, an output column that is a bare
//! column of a source holds that source row's value, so a condition the
//! statement puts on the column holds of the source rows too: a source row
//! failing it makes only output rows that fail it. When it groups them, a
//! column the GROUP BY lists holds the same value in every row of a group,
//! so the same holds of that column: a source row failing the condition is
//! in a group whose row fails it.

use crate::from_item::{Column, ColumnValue, Columns, FromItem};
use crate::schema_objects::read_view;
use crate::scope_edge::{Member, MemberValue};
use crate::scope_one_of::constant;
use crate::scope_plan::{is_name, read_select, Edge, Object, Planner, Source, Unread};
use crate::scope_term::{Affinity, ScopeTerm};
use crate::select_syntax::{parts, text, Part};
use rusqlite::Connection;
use sqlite3_parser::ast::{CommonTableExpr, Name, OneSelect, ResultColumn, Select, With};

/// The CTEs a FROM item can name where it is read, and those whose SQL is
/// being read now, which naming again would recurse.
#[derive(Clone, Default)]
pub(crate) struct Ctes<'a> {
    /// Each CTE visible here, each WITH's after those of the WITHs around
    /// it, so the innermost of a name is the last.
    visible: Vec<Declared<'a>>,
    /// The numbers of the CTEs whose SQL is being read.
    reading: Vec<usize>,
}

/// A CTE as one WITH declares it.
#[derive(Clone, Copy)]
pub(crate) struct Declared<'a> {
    cte: &'a CommonTableExpr<'a>,
    /// Its number among the statement's CTEs, which the FROM items that read
    /// it hold.
    pub(crate) number: usize,
    /// Whether its WITH sits in no other core, where its SQL can name no row
    /// around it.
    alone: bool,
    /// How many of the CTEs visible where it is declared its own SQL can
    /// name: those of its WITH, and those of the WITHs around it.
    scope: usize,
}

impl<'a> Ctes<'a> {
    /// These CTEs and `with`'s, which sits in no other core when `alone`,
    /// numbered from `numbered` on, which counts them. A CTE of an outer
    /// CTE's name stands for the inner one where the inner WITH's SQL names
    /// it, as SQLite resolves a name to the innermost CTE of that name.
    pub(crate) fn with(&self, with: &'a With<'a>, alone: bool, numbered: &mut usize) -> Self {
        let mut visible = self.visible.clone();
        let scope = visible.len() + with.ctes.len();
        for cte in with.ctes.iter() {
            visible.push(Declared { cte, number: *numbered, alone, scope });
            *numbered += 1;
        }
        Self { visible, reading: self.reading.clone() }
    }

    /// The CTE `name` names, the innermost of that name.
    pub(crate) fn named(&self, name: &Name<'_>) -> Option<Declared<'a>> {
        self.visible.iter().rev().find(|declared| declared.cte.tbl_name == *name).copied()
    }

    /// The CTEs `declared`'s own SQL can name, as SQLite resolves names
    /// where a CTE is declared, reading it; `None` when it is being read
    /// already, as a CTE that names itself, or one naming it, is.
    fn reading(&self, declared: &Declared<'a>) -> Option<Self> {
        if self.reading.contains(&declared.number) {
            return None;
        }
        let mut reading = self.reading.clone();
        reading.push(declared.number);
        Some(Self { visible: self.visible[..declared.scope].to_vec(), reading })
    }

    /// `select`, a SELECT that reads the CTEs numbered `numbers`, after a
    /// WITH of those visible here: one WITH for each WITH they are declared
    /// in, each around the next, so each CTE's SQL names what it names where
    /// it is declared, and a lookup's WITH holds no two CTEs of one name.
    /// `None` when none of them is visible here.
    fn around(&self, numbers: &[usize], select: &str) -> Option<String> {
        let mut withs: Vec<(usize, Vec<String>)> = Vec::new();
        for declared in self.visible.iter().filter(|declared| numbers.contains(&declared.number)) {
            match withs.last_mut() {
                Some((scope, ctes)) if *scope == declared.scope => ctes.push(text(declared.cte)),
                _ => withs.push((declared.scope, vec![text(declared.cte)])),
            }
        }
        let (_, innermost) = withs.pop()?;
        let mut sql = format!("WITH {} {select}", innermost.join(","));
        while let Some((_, ctes)) = withs.pop() {
            sql = format!("WITH {} SELECT * FROM ({sql})", ctes.join(","));
        }
        Some(sql)
    }
}

/// What a FROM item reads rows from, as SQL of its own.
pub(crate) enum Named<'a, 'v> {
    Subquery(&'a Select<'a>),
    Cte(Declared<'a>),
    /// One of the app's views, by its stored name.
    View(&'v str),
}

/// The cores of a subquery, CTE or view that a condition on its rows
/// narrows: for each such arm, the source and column each output column
/// passes on unchanged, if it does.
pub(crate) struct Derived {
    arms: Vec<Passed>,
    /// The same for every arm, whose rows each row of the query is one of,
    /// a LIMIT or not; `None` when an arm groups or aggregates, whose rows
    /// pass on no row's own value.
    rows: Option<Vec<Passed>>,
}

/// For each output column of an arm, the source of the arm's own and its
/// column the output passes on unchanged, if it does.
type Passed = Vec<Option<(usize, String)>>;

/// Read `named` as a FROM item of a core that sits in `parent`: its source,
/// its columns and its cores, or why sqlive can't read its rows.
pub(crate) fn read_derived_item<'a>(
    connection: &Connection,
    planner: &mut Planner,
    named: Named<'a, '_>,
    qualifier: String,
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Result<(Source, Columns, Derived), Unread>> {
    // What reading it adds to the planner: its own items and cores.
    let (items, cores) = (planner.items.len(), planner.cores.len());
    let found = match named {
        Named::Subquery(select) => {
            // A subquery reads the names around its core, not its core's.
            read_derived(connection, planner, select, None, parent, ctes)?.map(
                |(columns, derived)| {
                    let from = planner.alone(parent, items, cores).then(|| {
                        let wrapped = format!("SELECT * FROM ({})", text(select));
                        match ctes.around(&planner.ctes_read(items), &wrapped) {
                            Some(with) => format!("({with}) AS {qualifier}"),
                            None => format!("({}) AS {qualifier}", text(select)),
                        }
                    });
                    (from, columns, derived)
                },
            )
        }
        Named::Cte(declared) => {
            let cte = declared.cte;
            let name = crate::sql::name(cte.tbl_name.0).to_ascii_lowercase();
            // A CTE naming itself, or one naming it, recurses.
            let Some(inner) = ctes.reading(&declared) else {
                return Ok(Err(Unread("a recursive CTE")));
            };
            planner.inlined.push(name.clone());
            let around = planner.within.replace(name.clone());
            let names = cte.columns.map(|columns| {
                columns
                    .iter()
                    .map(|column| crate::sql::name(column.col_name.0).to_ascii_lowercase())
                    .collect()
            });
            let read = read_derived(connection, planner, cte.select, names, parent, &inner)?;
            planner.within = around;
            read.map(|(columns, derived)| {
                // A CTE of a WITH in no other core names no row around
                // it, wherever the statement reads it.
                let alone = declared.alone || planner.alone(parent, items, cores);
                let from = alone.then(|| {
                    let mut read = planner.ctes_read(items);
                    read.push(declared.number);
                    let select = format!("SELECT * FROM {}", crate::sql::identifier(&name));
                    let with =
                        ctes.around(&read, &select).expect("a CTE is visible where it is read");
                    format!("({with}) AS {qualifier}")
                });
                (from, columns, derived)
            })
        }
        Named::View(stored) => {
            let around = planner.within.replace(stored.to_ascii_lowercase());
            let read = read_view(connection, stored, |columns, select| {
                let names = columns.map(|columns| {
                    columns
                        .iter()
                        .map(|column| crate::sql::name(column.col_name.0).to_ascii_lowercase())
                        .collect()
                });
                // A view names only the schema's tables and views.
                read_derived(connection, planner, select, names, None, &Ctes::default())
            })?;
            planner.within = around;
            let Ok(read) = read else {
                return Ok(Err(Unread("a view the parser can't read")));
            };
            read.map(|(columns, derived)| {
                let from = Some(format!("main.{} AS {qualifier}", crate::sql::identifier(stored)));
                (from, columns, derived)
            })
        }
    };
    Ok(found.map(|(from, columns, derived)| {
        (
            Source {
                object: Object::Derived,
                from,
                qualifier,
                term: None,
                carried: None,
                either: Vec::new(),
                memberships: Vec::new(),
            },
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
) -> rusqlite::Result<Result<(Columns, Derived), Unread>> {
    let cores = match read_select(connection, planner, select, parent, ctes)? {
        Ok(cores) => cores,
        Err(unread) => return Ok(Err(unread)),
    };
    let bodies = std::iter::once(&select.body.select)
        .chain(select.body.compounds.iter().flatten().map(|compound| &compound.select));
    let mut arms = Vec::new();
    for (core, body) in cores.iter().copied().zip(bodies) {
        let Some(columns) = arm_columns(planner, core, body) else {
            // A `*` over a table function's columns: no column is known.
            return Ok(Ok((
                Columns::Unknown { volatile: planner.select_volatile(select) },
                Derived { arms: Vec::new(), rows: None },
            )));
        };
        arms.push((core, columns, arm_rows(planner, select, core, body)));
    }
    let width = arms[0].1.len();
    let named: Vec<Option<String>> = match names {
        Some(names) if names.len() == width => names.into_iter().map(Some).collect(),
        Some(_) => {
            return Ok(Err(Unread(
                "a CTE or view whose column list differs in length from its SELECT's columns",
            )))
        }
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
            let output = columns.get(position)?;
            match &output.value {
                OutputValue::Passed(item, column) => planner.items[*item].columns.affinity(column),
                OutputValue::Constant => Some(Affinity::Blob),
                OutputValue::Volatile | OutputValue::Other => None,
            }
        });
        let volatile = arms.iter().any(|(core, columns, _)| {
            // Every column of a core that reads a view read whole is one
            // sqlive can't judge.
            planner.reads_view_whole(*core)
                || columns.get(position).is_some_and(|output| match &output.value {
                    OutputValue::Volatile => true,
                    OutputValue::Passed(item, column) => {
                        planner.items[*item].columns.volatile(column)
                    }
                    OutputValue::Constant | OutputValue::Other => false,
                })
        });
        let first = affinities.next().flatten();
        let affinity = first.filter(|first| affinities.all(|other| other == Some(*first)));
        let value = if volatile {
            ColumnValue::Volatile
        } else {
            affinity.map_or(ColumnValue::Unknown, ColumnValue::Compared)
        };
        columns.push(Column { name, value });
    }
    let rows = (!arms.iter().any(|(_, _, rows)| rows.aggregated || rows.windowed)).then(|| {
        arms.iter()
            .map(|(_, columns, _)| columns.iter().map(|output| output.passed()).collect())
            .collect()
    });
    // A LIMIT keeps rows by the others around them, and a window function's
    // value in a row depends on the rows around it: a condition on such a
    // row says nothing of its sources. Grouping makes one row of many, which
    // hold the group's value only in a column the GROUP BY lists.
    let arms = if select.limit.is_some() {
        Vec::new()
    } else {
        arms.into_iter()
            .filter(|(_, _, rows)| !rows.windowed)
            .map(|(_, columns, rows)| {
                columns
                    .into_iter()
                    .map(|output| {
                        output
                            .passed()
                            .filter(|passed| !rows.aggregated || rows.keys.contains(passed))
                    })
                    .collect()
            })
            .collect()
    };
    Ok(Ok((Columns::Listed(columns), Derived { arms, rows })))
}

/// How an arm makes its rows from its sources' rows.
struct ArmRows {
    /// Whether it groups its rows or calls an aggregate, which makes one row
    /// of many.
    aggregated: bool,
    /// Whether it calls a window function, whose value in a row depends on
    /// the arm's other rows.
    windowed: bool,
    /// The columns its GROUP BY lists as bare columns of its own sources,
    /// whose collation is BINARY: each row of a group holds the group's
    /// value in them, where another collation could group values that
    /// differ.
    keys: Vec<(usize, String)>,
}

/// How `body`, the arm `core` of `select`, makes its rows.
fn arm_rows(planner: &Planner, select: &Select<'_>, core: usize, body: &OneSelect<'_>) -> ArmRows {
    let OneSelect::Select { columns, group_by, having, window_clause, .. } = body else {
        return ArmRows { aggregated: false, windowed: false, keys: Vec::new() };
    };
    let mut found = Vec::new();
    for column in columns.iter() {
        if let ResultColumn::Expr(expr, _) = column {
            parts(expr, &mut found);
        }
    }
    if select.body.compounds.is_none() {
        for term in select.order_by.iter().flat_map(|terms| terms.iter()) {
            parts(&term.expr, &mut found);
        }
    }
    let keys = group_by
        .iter()
        .flat_map(|exprs| exprs.iter())
        .filter(|expr| is_name(expr))
        .filter_map(|expr| planner.resolve(core, expr))
        .filter(|(item, column)| {
            planner.items[*item].core == core
                && planner.items[*item].columns.affinity(column).is_some()
        })
        .collect();
    ArmRows {
        aggregated: group_by.is_some()
            || having.is_some()
            || found.iter().any(|part| part.aggregates(planner.functions)),
        windowed: window_clause.is_some() || found.iter().any(Part::windowed),
        keys,
    }
}

/// A column a core outputs: its name as SQLite gives it, when sqlive knows
/// it, and the source of the core's own and the column of it the output
/// passes on unchanged, if it does.
struct Output {
    name: Option<String>,
    value: OutputValue,
}

/// What an output column holds.
enum OutputValue {
    /// A column of the core's own source, passed on unchanged.
    Passed(usize, String),
    /// A literal or a parameter, whose values have no affinity and compare
    /// by BINARY, as a column declared with no type. It holds no clock,
    /// which would give a write's lookup another answer than the query's
    /// (`Volatile`).
    Constant,
    /// An expression that gives another answer when evaluated again.
    Volatile,
    Other,
}

impl Output {
    fn passed(&self) -> Option<(usize, String)> {
        match &self.value {
            OutputValue::Passed(item, column) => Some((*item, column.clone())),
            OutputValue::Constant | OutputValue::Volatile | OutputValue::Other => None,
        }
    }
}

/// Each output column of the arm `core`, read from `body`, or `None` when a
/// `*` spans columns sqlive doesn't know.
fn arm_columns(planner: &Planner, core: usize, body: &OneSelect<'_>) -> Option<Vec<Output>> {
    let OneSelect::Select { columns, .. } = body else {
        let OneSelect::Values(rows) = body else { unreachable!("a core is a SELECT or VALUES") };
        // VALUES names its columns column1, column2, …
        let names = crate::sql::values_names(
            rows.first().expect("a parsed VALUES has at least one row").len(),
        );
        let mut found = Vec::new();
        for (position, name) in names.into_iter().enumerate() {
            let mut volatile = false;
            for expr in rows.iter().filter_map(|row| row.get(position)) {
                volatile |= volatile_expr(planner, core, expr);
            }
            let value = if volatile { OutputValue::Volatile } else { OutputValue::Other };
            found.push(Output { name: Some(name), value });
        }
        return Some(found);
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
                    Some(alias) => Some(crate::sql::alias_name(alias)),
                    None => passed.as_ref().map(|(_, column)| column.clone()),
                };
                let value = match passed {
                    Some((item, column)) => OutputValue::Passed(item, column),
                    None if volatile_expr(planner, core, expr) => OutputValue::Volatile,
                    None if constant(expr) => OutputValue::Constant,
                    None => OutputValue::Other,
                };
                found.push(Output { name, value });
            }
            ResultColumn::Star => {
                // A USING join's right side leaves out the columns it
                // compares, which the item on the left lists: among columns
                // of names sqlive doesn't know, it can't tell which.
                for item in &planner.cores[core].items {
                    let using = &planner.items[*item].using;
                    if !using.is_empty() && !planner.items[*item].columns.complete() {
                        return None;
                    }
                    let columns = every_column(&planner.items[*item], *item)?;
                    found.extend(columns.into_iter().filter(|output| {
                        output.name.as_ref().is_none_or(|name| !using.contains(name))
                    }));
                }
            }
            ResultColumn::TableStar(qualifier) => {
                let named = planner.cores[core]
                    .items
                    .iter()
                    .copied()
                    .find(|item| *qualifier == Name(&planner.items[*item].source.qualifier));
                let item = named?;
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
                    value: match &column.name {
                        Some(name) => OutputValue::Passed(position, name.clone()),
                        None => OutputValue::Other,
                    },
                })
                .collect(),
        ),
        Columns::Unknown { .. } => None,
    }
}

/// What the arms of the subquery, CTE or view `item` say of its column
/// `column`: in each arm, the term of the source the column passes on, as a
/// term on `column`, joined with OR. Each row of its query is a row of an
/// arm, which holds that source's value in the column, so the term holds of
/// the row when the column isn't NULL. `None` when an arm groups, says
/// nothing of the column, or passes on no column of its own source.
pub(crate) fn pulled(planner: &Planner, item: usize, column: &str) -> Option<ScopeTerm> {
    let rows = planner.items[item].derived.as_ref()?.rows.as_ref()?;
    let Columns::Listed(columns) = &planner.items[item].columns else {
        return None;
    };
    let position = columns.iter().position(|listed| listed.name.as_deref() == Some(column))?;
    let mut terms = rows.iter().map(|arm| {
        let (source, passed) = arm.get(position)?.as_ref()?;
        let term = planner.items[*source].source.term.as_ref()?;
        term.renamed(&|tested| (tested == passed).then(|| column.to_owned()))
    });
    let first = terms.next()??;
    let rest: Option<Vec<ScopeTerm>> = terms.collect();
    // Joined from the last, as the model's `orAll` joins them.
    let mut joined: Vec<ScopeTerm> = std::iter::once(first).chain(rest?).collect();
    let mut term = joined.pop().expect("one term at least");
    while let Some(before) = joined.pop() {
        term = ScopeTerm::Or(Box::new(before), Box::new(term));
    }
    Some(term)
}

/// Narrow the cores of the subquery, CTE or view `item` by `term` and
/// `edges`, which its own core's conditions put on its rows: each arm's
/// source an output column passes on takes them as its own. A LEFT JOIN's
/// right side in the arm does too, and is an inner join's then: a row with
/// NULLs in its place fails each of them, as a WHERE condition in the arm
/// would. A term a row of NULLs can pass, as a NULL test, is the exception:
/// it narrows nothing there and leaves the join a LEFT JOIN.
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
        let reached = tested
            .iter()
            .map(String::as_str)
            .chain(edges.iter().filter_map(|edge| edge.member.column()));
        for column in reached {
            if let Some((target, _)) = through(column) {
                if !targets.contains(&target) {
                    targets.push(target);
                }
            }
        }
        for target in targets {
            let found = &planner.items[target];
            if found.source.table().is_none() && found.derived.is_none() {
                continue;
            }
            // A term a row of NULLs can pass narrows nothing on a LEFT JOIN's
            // right side that stays one, and doesn't make it inner.
            let left = planner.items[target].left_on.is_some();
            let renamed = term
                .as_ref()
                .and_then(|term| {
                    term.renamed(&|column| {
                        through(column)
                            .filter(|(passed, _)| *passed == target)
                            .map(|(_, column)| column)
                    })
                })
                .filter(|renamed| !(left && renamed.may_hold_on_nulls()));
            // An `IS` edge is the same.
            let passed: Vec<Edge> = edges
                .iter()
                .filter(|edge| !(left && edge.nulls))
                .filter_map(|edge| {
                    let (passed, column) = through(edge.member.column()?)?;
                    let member = Member {
                        item: target,
                        value: MemberValue::Column(column),
                        affinity: edge.member.affinity,
                    };
                    (passed == target).then(|| Edge { member, ..edge.clone() })
                })
                .collect();
            if let Some(renamed) = &renamed {
                ScopeTerm::add_to(&mut planner.items[target].source.term, renamed.clone());
            }
            planner.edges.extend(passed.iter().cloned());
            if renamed.is_some() || !passed.is_empty() {
                planner.join_inner(target);
            }
            push_down(planner, target, renamed, passed);
        }
    }
}

/// Whether `expr`, written in `core`, is volatile (`Planner::volatile`).
fn volatile_expr(planner: &Planner, core: usize, expr: &sqlite3_parser::ast::Expr<'_>) -> bool {
    let mut found = Vec::new();
    parts(expr, &mut found);
    planner.volatile(core, &found)
}
