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
use crate::from_star::StarItem;
use crate::scope_edge::Edge;
use crate::scope_one_of::constant;
use crate::scope_or::Side;
use crate::scope_plan::{is_name, read_select, Object, Planner, Source, Unread};
use crate::scope_term::{Affinity, ScopeTerm};
use crate::select_syntax::{parts, text, Part};
use rusqlite::Connection;
use sqlite3_parser::ast::{
    CommonTableExpr, CompoundOperator, Distinctness, Expr, Name, OneSelect, ResultColumn, Select,
    With,
};

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
    pub(crate) cte: &'a CommonTableExpr<'a>,
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
    pub(crate) fn reading(&self, declared: &Declared<'a>) -> Option<Self> {
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
    /// Each arm's core.
    pub(crate) cores: Vec<usize>,
    /// For each arm a condition on its rows passes into.
    pub(crate) arms: Vec<Passed>,
    /// The arms whose rows are the same without a column nothing reads
    /// (`unread_columns.rs`).
    prunable: Vec<usize>,
    /// The same for every arm, whose rows each row of the query is one of,
    /// a LIMIT or not; `None` when an arm groups or aggregates, whose rows
    /// pass on no row's own value.
    rows: Option<Vec<Passed>>,
}

impl Derived {
    pub(crate) fn prunable_arms(&self) -> &[usize] {
        &self.prunable
    }
}

/// For each output column of an arm, the source of the arm's own and its
/// column the output passes on unchanged, if it does.
type Passed = Vec<Option<(usize, String)>>;

/// What each of `arms` passes on in the column `column` of `columns`, the
/// columns of the query they are the arms of; nothing when `columns` lists no
/// such column.
fn passed_on<'a>(
    columns: &Columns,
    arms: &'a [Passed],
    column: &str,
) -> Vec<Option<&'a (usize, String)>> {
    let Columns::Listed(columns) = columns else {
        return Vec::new();
    };
    let Some(position) = columns.iter().position(|listed| listed.name.as_deref() == Some(column))
    else {
        return Vec::new();
    };
    arms.iter().map(|arm| arm.get(position)?.as_ref()).collect()
}

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
            let names = cte.columns.map(crate::sql::column_list_names);
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
            let read = planner.schema.read_view(stored, |columns, select| {
                let names = columns.map(crate::sql::column_list_names);
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
                unread: false,
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
                Derived { cores, arms: Vec::new(), prunable: Vec::new(), rows: None },
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
    Ok(Ok((
        Columns::Listed(columns),
        Derived { prunable: prunable_arms(select, &cores), cores, arms, rows },
    )))
}

/// The cores among `cores`, `select`'s arms, whose rows don't depend on each
/// of their columns: those of a SELECT with no ORDER BY and no compound but
/// UNION ALL that is neither DISTINCT nor grouped.
fn prunable_arms(select: &Select<'_>, cores: &[usize]) -> Vec<usize> {
    let all_kept = select.order_by.is_none()
        && select
            .body
            .compounds
            .iter()
            .flatten()
            .all(|compound| compound.operator == CompoundOperator::UnionAll);
    if !all_kept {
        return Vec::new();
    }
    let bodies = std::iter::once(&select.body.select)
        .chain(select.body.compounds.iter().flatten().map(|compound| &compound.select));
    cores
        .iter()
        .copied()
        .zip(bodies)
        .filter(|(_, body)| {
            matches!(body, OneSelect::Select { distinctness, group_by: None, .. }
                if *distinctness != Some(Distinctness::Distinct))
        })
        .map(|(core, _)| core)
        .collect()
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
pub(crate) struct Output<'a> {
    name: Option<String>,
    value: OutputValue,
    /// The expression that writes it; `None` for one a `*` or VALUES gives.
    written: Option<&'a Expr<'a>>,
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

impl<'a> Output<'a> {
    pub(crate) fn written(&self) -> Option<&'a Expr<'a>> {
        self.written
    }

    pub(crate) fn passed(&self) -> Option<(usize, String)> {
        match &self.value {
            OutputValue::Passed(item, column) => Some((*item, column.clone())),
            OutputValue::Constant | OutputValue::Volatile | OutputValue::Other => None,
        }
    }
}

/// Each output column of the arm `core`, read from `body`, or `None` when a
/// `*` spans columns sqlive doesn't know.
pub(crate) fn arm_columns<'a>(
    planner: &Planner,
    core: usize,
    body: &'a OneSelect<'a>,
) -> Option<Vec<Output<'a>>> {
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
            found.push(Output { name: Some(name), value, written: None });
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
                found.push(Output { name, value, written: Some(expr) });
            }
            ResultColumn::Star => {
                let items = &planner.cores[core].items;
                let listed =
                    crate::from_star::star(items.iter().map(|item| &planner.items[*item]))?;
                found.extend(
                    listed.into_iter().map(|(at, column)| passed_through(items[at], column)),
                );
            }
            ResultColumn::TableStar(qualifier) => {
                let named = planner.cores[core]
                    .items
                    .iter()
                    .copied()
                    .find(|item| *qualifier == Name(&planner.items[*item].source.qualifier));
                let item = named?;
                let listed = planner.items[item].listed()?;
                found.extend(listed.iter().map(|column| passed_through(item, column)));
            }
        }
    }
    Some(found)
}

/// The output of a `*` that gives `column` of the item at `position`.
fn passed_through(position: usize, column: &Column) -> Output<'static> {
    Output {
        written: None,
        name: column.name.clone(),
        value: match &column.name {
            Some(name) => OutputValue::Passed(position, name.clone()),
            None => OutputValue::Other,
        },
    }
}

impl StarItem for FromItem {
    type Column = Column;

    fn listed(&self) -> Option<&[Column]> {
        match &self.columns {
            Columns::Listed(columns) => Some(columns),
            Columns::Unknown { .. } => None,
        }
    }

    fn using(&self) -> &[String] {
        &self.using
    }

    fn name(column: &Column) -> Option<&str> {
        column.name.as_deref()
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
    let mut terms = passed_on(&planner.items[item].columns, rows, column).into_iter().map(|arm| {
        let (source, passed) = arm?;
        let term = planner.items[*source].source.term.as_ref()?;
        term.renamed(&|tested| (tested == passed).then(|| column.to_owned()), None)
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

/// The source, and its column, that `item`'s column `column` passes on
/// unchanged, when `item` is a subquery or CTE that can't be read alone, of
/// one arm that neither aggregates nor calls a window function, and sqlive
/// can read that source alone. Each row of such an item is a row of its arm,
/// so its column holds the value the source's row gives in a binding of the
/// arm, and an equality with the column takes values from the source as
/// one with the source's column would.
pub(crate) fn passed_source(
    planner: &Planner,
    item: usize,
    column: &str,
) -> Option<(usize, String)> {
    let found = &planner.items[item];
    if found.source.from.is_some() {
        return None;
    }
    let rows = found.derived.as_ref()?.rows.as_ref()?;
    let [Some((source, passed))] = passed_on(&found.columns, rows, column)[..] else {
        return None;
    };
    planner.items[*source].source.from.as_ref()?;
    Some((*source, passed.clone()))
}

/// A table, or a subquery, CTE or view, that one arm of a derived item passes
/// columns on from.
struct ArmTarget<'a> {
    item: usize,
    /// Each column of the derived item that the arm passes on from `item`,
    /// with the column of `item` it passes on.
    columns: Vec<(&'a str, String)>,
}

impl ArmTarget<'_> {
    /// The column of the target that the arm passes on as the derived item's
    /// column `reached`.
    fn column(&self, reached: &str) -> Option<String> {
        self.columns.iter().find(|(from, _)| *from == reached).map(|(_, column)| column.clone())
    }
}

/// The targets that `arm` of the derived item `item` passes the columns
/// `reached` on from, in the order those columns first name them: what a
/// condition on those columns can pass into.
fn arm_targets<'a>(
    planner: &Planner,
    item: usize,
    arm: &Passed,
    reached: &[&'a str],
) -> Vec<ArmTarget<'a>> {
    let mut targets: Vec<ArmTarget<'a>> = Vec::new();
    for &column in reached {
        let passed = passed_on(&planner.items[item].columns, std::slice::from_ref(arm), column);
        let Some((source, passed)) = passed.into_iter().next().flatten() else { continue };
        let found = &planner.items[*source];
        if found.source.table().is_none() && found.derived.is_none() {
            continue;
        }
        let entry = (column, passed.clone());
        match targets.iter_mut().find(|target| target.item == *source) {
            Some(target) => target.columns.push(entry),
            None => targets.push(ArmTarget { item: *source, columns: vec![entry] }),
        }
    }
    targets
}

/// The targets of `arm_targets` that no LEFT JOIN adds: those a carried term
/// or an OR passes into, as the NULLs such a join gives a left row pass
/// neither.
fn inner_targets<'a>(
    planner: &Planner,
    item: usize,
    arm: &Passed,
    reached: &[&'a str],
) -> Vec<ArmTarget<'a>> {
    let mut targets = arm_targets(planner, item, arm, reached);
    targets.retain(|target| planner.items[target.item].always_bound());
    targets
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
    let mut tested = Vec::new();
    if let Some(term) = &term {
        term.columns(&mut tested);
    }
    let reached: Vec<&str> = tested
        .iter()
        .map(String::as_str)
        .chain(edges.iter().flat_map(|edge| edge.member.columns()))
        .collect();
    let arms: Vec<Vec<ArmTarget>> =
        derived.arms.iter().map(|arm| arm_targets(planner, item, arm, &reached)).collect();
    for targets in arms {
        for passing in targets {
            let target = passing.item;
            let found = &planner.items[target];
            // A term a row of NULLs can pass narrows nothing on a LEFT JOIN's
            // right side that stays one, and doesn't make it inner.
            let left = found.left_on.is_some();
            let rows = &found.source.qualifier;
            let column = |column: &str| passing.column(column);
            let renamed = term
                .as_ref()
                .and_then(|term| term.renamed(&column, Some(rows)))
                .filter(|renamed| !(left && renamed.may_hold_on_nulls()));
            // An `IS` edge is the same.
            let missing = !found.always_bound();
            let passed: Vec<Edge> = edges
                .iter()
                .filter(|edge| !(left && edge.nulls))
                .filter_map(|edge| {
                    let member = edge.member.over(target, rows, missing, &column)?;
                    Some(Edge { member, pushed: true, ..edge.clone() })
                })
                .collect();
            if let Some(renamed) = &renamed {
                ScopeTerm::add_to(&mut planner.items[target].source.term, renamed.clone());
            }
            for edge in &passed {
                planner.add_edge(edge.clone());
            }
            if renamed.is_some() || !passed.is_empty() {
                planner.join_inner(target);
            }
            push_down(planner, target, renamed, passed);
        }
    }
}

/// Pass `term`, which the edges carry to the subquery, CTE or view `item`,
/// into the items its arms pass their columns on from, and through them
/// further down: each takes what the term says of it as a term its own edges
/// carry (`Planner::carry_terms`), unless a LEFT JOIN adds it. Each row of
/// the item the core keeps passes the term, so a row of an arm's source that
/// fails it gives no such row. The model states the rule as `pushCarried` in
/// `crates/sqlive/proofs`.
pub(crate) fn push_carried(planner: &mut Planner, item: usize, term: &ScopeTerm) {
    let Some(derived) = &planner.items[item].derived else {
        return;
    };
    let mut tested = Vec::new();
    term.columns(&mut tested);
    let reached: Vec<&str> = tested.iter().map(String::as_str).collect();
    let mut given: Vec<(usize, ScopeTerm)> = Vec::new();
    for arm in &derived.arms {
        for passing in inner_targets(planner, item, arm, &reached) {
            let rows = &planner.items[passing.item].source.qualifier;
            let renamed = term.renamed(&|column| passing.column(column), Some(rows));
            given.extend(renamed.map(|renamed| (passing.item, renamed)));
        }
    }
    for (target, term) in given {
        ScopeTerm::add_to(&mut planner.items[target].source.carried, term.clone());
        push_carried(planner, target, &term);
    }
}

/// Pass `ors`, the ORs the subquery, CTE or view `item` keeps as it is
/// placed, into the items its arms pass their columns on from: each arm's
/// source takes an OR each of whose sides says something of it, unless a LEFT
/// JOIN adds it, as the NULLs such a join gives a left row can pass no side.
/// An OR goes one level: a source that takes one passes it on as it keeps it,
/// when it is placed. The model states the rule as `pushOrs` in
/// `crates/sqlive/proofs`.
pub(crate) fn push_ors(planner: &mut Planner, item: usize, ors: &[Vec<Side>]) {
    let Some(derived) = &planner.items[item].derived else {
        return;
    };
    let mut given: Vec<(usize, Vec<Side>)> = Vec::new();
    for arm in &derived.arms {
        for sides in ors {
            let columns: Vec<String> = sides.iter().flat_map(Side::columns).collect();
            let reached: Vec<&str> = columns.iter().map(String::as_str).collect();
            for passing in inner_targets(planner, item, arm, &reached) {
                let rows = &planner.items[passing.item].source.qualifier;
                let column = |column: &str| passing.column(column);
                let sides: Option<Vec<Side>> =
                    sides.iter().map(|side| side.over(passing.item, rows, &column)).collect();
                given.extend(sides.map(|sides| (passing.item, sides)));
            }
        }
    }
    for (target, sides) in given {
        planner.items[target].ors.push(sides);
    }
}

/// The items the subquery, CTE or view `item` can pass the edges and ORs it
/// takes into, once it is placed (`push_down`, `push_ors`): those an arm
/// passes on a column of `compared` from, the columns its edges compare and
/// its ORs name, and, through a subquery,
/// CTE or view among them, those below it. The model states the rule as
/// `receivers` in `crates/sqlive/proofs`.
pub(crate) fn receivers<'a>(
    planner: &Planner,
    item: usize,
    compared: impl IntoIterator<Item = &'a str>,
) -> Vec<usize> {
    let mut found = Vec::new();
    for column in compared {
        receive(planner, item, column, &mut found);
    }
    found
}

/// Adds to `found` the items a condition on `item`'s column `column` passes
/// into.
fn receive(planner: &Planner, item: usize, column: &str, found: &mut Vec<usize>) {
    let Some(derived) = &planner.items[item].derived else {
        return;
    };
    let passed = passed_on(&planner.items[item].columns, &derived.arms, column);
    for (target, passed) in passed.into_iter().flatten() {
        let into = &planner.items[*target];
        if into.source.table().is_some() || into.derived.is_some() {
            found.push(*target);
            receive(planner, *target, passed, found);
        }
    }
}

/// Whether `expr`, written in `core`, is volatile (`Planner::volatile`).
fn volatile_expr(planner: &Planner, core: usize, expr: &Expr<'_>) -> bool {
    let mut found = Vec::new();
    parts(expr, &mut found);
    planner.volatile(core, &found)
}
