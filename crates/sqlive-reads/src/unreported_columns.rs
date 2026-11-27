//! The columns a statement's rows depend on that SQLite's authorizer doesn't
//! report as read (SPEC.md section 4). There are two kinds.
//!
//! The columns a USING or NATURAL join compares. A statement's reads add them
//! for each table the join compares directly, in any SELECT of the statement
//! and in any of the app's views it reads. A view's or a subquery's columns
//! the join compares are named in its own SELECT, where the authorizer reports
//! the columns of its tables itself.
//!
//! Every column of each table that a query whose rows SQLite picks reads. A
//! query keeps some of its rows by a LIMIT or an OFFSET, or as a subquery whose
//! first row an expression takes, and SQLite keeps the rows it reads first, in
//! the order of the plan it chooses: an index on a column the statement never
//! names can decide which rows those are. An ORDER BY that, with the
//! equalities of the query's conditions, tells every two of its rows apart
//! (`told_apart.rs`) leaves that order no choice, so such a query depends only
//! on the columns it names, as does one that aggregates with no GROUP BY,
//! which returns one row. The tables a query reads are those of its FROM, of
//! the views and CTEs it reads, and of every query inside it. The model states
//! the rule as `Statement.picks` and proves it in
//! `crates/sqlive/proofs/SqliveNarrowing/Rules/Pick.lean`.
//!
//! A core that aggregates or calls a window function picks rows too, so every
//! column of each table it reads counts as read when its rows follow the
//! order SQLite reads its rows in. A window function numbers or reads the
//! rows of a partition in that order among those its ORDER BY sorts the
//! same, unless that ORDER BY tells the core's rows apart in a core that
//! doesn't aggregate. A result column, the HAVING or
//! an ORDER BY term that names a column
//! outside every aggregate call and GROUP BY term takes the column's value
//! from one row of its group, which that order decides. A GROUP BY term, and
//! min and max, keep the value they read first among values that compare
//! equal, so a result column, the HAVING or an ORDER BY term that shows the
//! term follows that order, and so does a call of min or max, unless the term
//! or the argument is a column whose equal values are the same, and any other
//! aggregate but count reads its values in that order, as `group_concat`
//! joins them, unless the call's own ORDER BY tells the core's rows apart. The
//! model states the rule as `Statement.followsOrder`.
//!
//! DISTINCT, UNION, INTERSECT and EXCEPT keep one of each set of rows that
//! compare equal, which the order SQLite reads them in decides, so every
//! column of each table a DISTINCT core or a compound reads counts as read
//! unless each result column of the core, or of each arm, is a column whose
//! equal values are the same. The model states the rule as
//! `Statement.distinctFollowsOrder` and `Statement.compoundFollowsOrder`.
//!
//! Only what the statement runs counts: SQLite never runs a CTE that no query
//! of the statement reads, so its joins and the rows it would pick add no
//! column.
//!
//! When the parser can't read the statement or a view it reads, the columns
//! are unknown, and every column of every table counts as read.

use std::collections::BTreeSet;

use crate::read_set::ColumnSet;
use crate::schema_objects::MainSchema;
use crate::select_syntax::{
    aggregate_call, order_term, parts, parts_outside, window_call, AggregateCall, Functions,
    OrderTerm, Part,
};
use crate::sql_walk::{Reads, Scope, Visit, Walk, Within};
use crate::told_apart::{Item, OuterItems, Telling};
use rusqlite::Connection;
use sqlite3_parser::ast::{
    CompoundOperator, Distinctness, Expr, FromClause, JoinConstraint, JoinOperator, JoinType, Name,
    OneSelect, Over, ResultColumn, Select, SelectTable, SortedColumn, WindowDef,
};

/// The columns a statement's rows depend on beyond those the authorizer
/// reports, as (table, column) pairs, lowercase.
#[derive(Clone)]
pub(crate) enum UnreportedColumns {
    Known(ColumnSet),
    /// The parser couldn't read the statement or a view it reads: every
    /// column of every table in the main schema (`MAIN_TABLES`), so every
    /// write reruns the statement.
    Unknown(ColumnSet),
}

impl UnreportedColumns {
    /// The columns to count as read.
    pub(crate) fn columns(&self) -> &ColumnSet {
        match self {
            Self::Known(columns) | Self::Unknown(columns) => columns,
        }
    }

    /// Unknown columns, for the reason `why`, which goes to the log.
    pub(crate) fn unknown(schema: &MainSchema, why: String) -> rusqlite::Result<Self> {
        tracing::warn!(
            %why,
            "the planner can't tell which columns a statement's rows depend on, so every \
             column of every table counts as read"
        );
        let mut every = ColumnSet::new();
        for table in schema.tables() {
            for column in schema.columns(table)? {
                every.insert((table.to_ascii_lowercase(), column.name.to_ascii_lowercase()));
            }
        }
        Ok(Self::Unknown(every))
    }
}

/// The columns `select`'s rows depend on that the authorizer doesn't report,
/// with the connection's `functions`.
pub(crate) fn unreported_columns(
    connection: &Connection,
    schema: &MainSchema,
    functions: &Functions,
    select: &Select<'_>,
) -> rusqlite::Result<UnreportedColumns> {
    let found = Found {
        telling: Telling { connection, schema, functions },
        outer: Vec::new(),
        picked: Vec::new(),
        found: Vec::new(),
    };
    let mut walk = Walk::new(schema, found);
    let reads = walk.select(select, false)?;
    if let Some(why) = walk.scope.unread() {
        return UnreportedColumns::unknown(schema, why.to_owned());
    }
    let run = walk.scope.run(&reads);
    let counts =
        |within: &Option<Within>| within.as_ref().is_none_or(|within| run.contains(within));
    let mut found: ColumnSet = walk
        .visit
        .found
        .iter()
        .filter(|(within, _)| counts(within))
        .map(|(_, column)| column.clone())
        .collect();
    let mut picked = BTreeSet::new();
    for (_, reads) in walk.visit.picked.iter().filter(|(within, _)| counts(within)) {
        picked.extend(walk.scope.tables(reads));
    }
    for table in picked {
        for column in walk.visit.columns(&table)? {
            found.insert((table.to_ascii_lowercase(), column));
        }
    }
    Ok(UnreportedColumns::Known(found))
}

/// What the walk finds of the columns the authorizer doesn't report, each
/// with the CTE or view whose SQL holds it: what the walk finds there
/// counts only when the statement runs it.
struct Found<'c> {
    telling: Telling<'c>,
    /// The names of each SELECT's FROM items around where the walk is, the
    /// innermost last.
    outer: Vec<OuterItems>,
    /// What each query whose rows SQLite picks reads, and where it is.
    picked: Vec<(Option<Within>, Reads)>,
    /// The columns USING and NATURAL joins compare, and where each join is.
    found: Vec<(Option<Within>, (String, String))>,
}

impl Visit for Found<'_> {
    fn enter(&mut self, scope: &Scope<'_>, core: &OneSelect<'_>) -> rusqlite::Result<()> {
        let from = match core {
            OneSelect::Select { from, .. } => from.as_ref(),
            OneSelect::Values(_) => None,
        };
        let items = self.telling.items(scope, &self.outer, from)?;
        self.outer.push(Telling::outer_items(items));
        Ok(())
    }

    fn leave(&mut self) {
        self.outer.pop();
    }

    /// Note `select` when SQLite picks its rows.
    fn select(
        &mut self,
        scope: &Scope<'_>,
        select: &Select<'_>,
        first_row: bool,
        reads: &Reads,
    ) -> rusqlite::Result<()> {
        if (select.limit.is_some() || first_row)
            && !self.telling.query_tells_apart(scope, &self.outer, select)?
            || self.compound_follows_order(scope, select)?
        {
            self.picked.push((scope.within(), reads.clone()));
        }
        Ok(())
    }

    /// Note `core` when its rows follow the order SQLite reads its rows in.
    fn core(
        &mut self,
        scope: &Scope<'_>,
        core: &OneSelect<'_>,
        order_by: &[SortedColumn<'_>],
        reads: &Reads,
    ) -> rusqlite::Result<()> {
        if self.follows_order(scope, core, order_by)? || self.distinct_follows_order(scope, core)? {
            self.picked.push((scope.within(), reads.clone()));
        }
        Ok(())
    }

    /// Each USING or NATURAL join's columns: a USING column of the joined
    /// table and of the leftmost item before the join that has it, which
    /// SQLite compares it with, and every column of each table before the
    /// join or joined for a NATURAL join, which compares the columns the two
    /// sides share.
    fn from(
        &mut self,
        scope: &Scope<'_>,
        from: &FromClause<'_>,
        items: &[&SelectTable<'_>],
    ) -> rusqlite::Result<()> {
        for (position, join) in from.joins.iter().flatten().enumerate() {
            // The items before the join's own, which sits after the first.
            let (before, joined) = (&items[..=position], items[position + 1]);
            if matches!(
                join.operator,
                JoinOperator::TypedJoin(Some(kind)) if kind.contains(JoinType::NATURAL)
            ) {
                for item in before.iter().chain([&joined]) {
                    if let Some((table, columns)) = self.table(scope, item)? {
                        for column in columns {
                            self.found.push((scope.within(), (table.to_ascii_lowercase(), column)));
                        }
                    }
                }
                continue;
            }
            let Some(JoinConstraint::Using(names)) = &join.constraint else { continue };
            for name in names.iter() {
                let column = crate::sql::name(name.0).to_ascii_lowercase();
                self.table_column(scope, joined, &column)?;
                // Past an item whose columns this doesn't know, any table
                // after it could be the one compared.
                let mut known = true;
                for item in before {
                    match self.has_column(scope, item, &column)? {
                        Some(false) => {}
                        Some(true) => {
                            self.table_column(scope, item, &column)?;
                            if known {
                                break;
                            }
                        }
                        None => known = false,
                    }
                }
            }
        }
        Ok(())
    }
}

impl Found<'_> {
    /// The table `item` reads directly, and its columns, lowercase.
    fn table(
        &self,
        scope: &Scope<'_>,
        item: &SelectTable<'_>,
    ) -> rusqlite::Result<Option<(String, Vec<String>)>> {
        let SelectTable::Table(name, ..) = item else { return Ok(None) };
        let Some(table) = scope.object(name, &["table", "virtual"]) else { return Ok(None) };
        let columns = self.columns(table)?;
        Ok(Some((table.to_owned(), columns)))
    }

    /// Count `column` of the table `item` reads directly, when it has one.
    fn table_column(
        &mut self,
        scope: &Scope<'_>,
        item: &SelectTable<'_>,
        column: &str,
    ) -> rusqlite::Result<()> {
        if let Some((table, columns)) = self.table(scope, item)? {
            if columns.iter().any(|found| found == column) {
                let column = (table.to_ascii_lowercase(), column.to_owned());
                self.found.push((scope.within(), column));
            }
        }
        Ok(())
    }

    /// Whether `item` has a column named `column`, when this knows: a table
    /// or a view of the main schema, or a subquery whose result columns
    /// SQLite names by their aliases or the columns they name.
    fn has_column(
        &self,
        scope: &Scope<'_>,
        item: &SelectTable<'_>,
        column: &str,
    ) -> rusqlite::Result<Option<bool>> {
        let names = match item {
            SelectTable::Table(name, ..) => {
                let Some(object) = scope.object(name, &["table", "view", "virtual"]) else {
                    return Ok(None);
                };
                self.columns(object)?
            }
            SelectTable::Select(select, _) => {
                let Some(names) = result_names(select) else { return Ok(None) };
                names
            }
            SelectTable::TableCall(..) | SelectTable::Sub(..) => return Ok(None),
        };
        Ok(Some(names.iter().any(|found| found == column)))
    }

    /// Whether `core` is DISTINCT, which keeps one of each set of rows that
    /// compare equal, the one SQLite reads first, and one of its result
    /// columns can hold values that compare equal and differ: it is no column
    /// whose equal values are the same (`returns_exactly`).
    fn distinct_follows_order(
        &self,
        scope: &Scope<'_>,
        core: &OneSelect<'_>,
    ) -> rusqlite::Result<bool> {
        let OneSelect::Select { distinctness: Some(Distinctness::Distinct), .. } = core else {
            return Ok(false);
        };
        Ok(!self.returns_exactly(scope, core)?)
    }

    /// Whether `select` combines its arms by UNION, INTERSECT or EXCEPT, which
    /// keep one of each set of rows that compare equal, the one the order
    /// SQLite reads them in decides, and a result column of an arm can hold
    /// values that compare equal and differ: it is no column whose equal values
    /// are the same (`returns_exactly`).
    fn compound_follows_order(
        &self,
        scope: &Scope<'_>,
        select: &Select<'_>,
    ) -> rusqlite::Result<bool> {
        let Some(compounds) = &select.body.compounds else { return Ok(false) };
        if compounds.iter().all(|compound| compound.operator == CompoundOperator::UnionAll) {
            return Ok(false);
        }
        let arms = std::iter::once(&select.body.select)
            .chain(compounds.iter().map(|compound| &compound.select));
        for arm in arms {
            if !self.returns_exactly(scope, arm)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether each result column of `core` is a column whose equal values
    /// are the same (`compares_exactly`) of a table of its FROM.
    fn returns_exactly(&self, scope: &Scope<'_>, core: &OneSelect<'_>) -> rusqlite::Result<bool> {
        let OneSelect::Select { columns, from, .. } = core else { return Ok(false) };
        let Some(items) = self.telling.items(scope, &self.outer, from.as_ref())? else {
            return Ok(false);
        };
        for column in columns.iter() {
            let ResultColumn::Expr(expr, _) = column else { return Ok(false) };
            if !self.compares_exactly(&items, expr)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether the expression `expr` names a column among `items` whose equal
    /// values are the same.
    fn compares_exactly(&self, items: &[Item<'_>], expr: &Expr<'_>) -> rusqlite::Result<bool> {
        match Telling::own_column(items, &self.outer, expr) {
            Some((item, column)) => self.telling.compares_exactly(items, item, &column),
            None => Ok(false),
        }
    }

    /// Whether the rows of a core follow the order SQLite reads its rows in.
    /// A window function numbers or reads the rows of a partition in that
    /// order among those its ORDER BY sorts the same, unless the ORDER BY
    /// tells the core's rows apart, in a core that doesn't aggregate. In a core that aggregates, a result
    /// column, the HAVING or a term of the
    /// ORDER BY `order_by` takes its value from one row of each group, which
    /// that order decides, when it names a column, or holds a subquery that
    /// can, outside every aggregate call and GROUP BY term, and so does a
    /// `*`. A GROUP BY term, and min and max, keep the value they read first
    /// among values that compare equal, so a result column, the HAVING or an
    /// ORDER BY term that shows the term follows that order, and so does a
    /// call of min or max, unless the term or the argument is a column whose
    /// equal values are the same (`compares_exactly`). Any other aggregate but count reads
    /// its values in that order too, as `group_concat` joins them and a sum
    /// of reals adds them, unless its own ORDER BY tells the core's rows
    /// apart.
    fn follows_order(
        &self,
        scope: &Scope<'_>,
        core: &OneSelect<'_>,
        order_by: &[SortedColumn<'_>],
    ) -> rusqlite::Result<bool> {
        let OneSelect::Select { columns, from, group_by, having, window_clause, .. } = core else {
            return Ok(false);
        };
        let mut evaluated = Vec::new();
        let mut star = false;
        for column in columns.iter() {
            match column {
                ResultColumn::Expr(expr, _) => evaluated.push(expr),
                ResultColumn::Star | ResultColumn::TableStar(_) => star = true,
            }
        }
        evaluated.extend(having.iter().copied());
        for term in order_by {
            // A term that names a result column sorts by its expression,
            // which the result columns hold.
            if let OrderTerm::Expr(expr) = order_term(columns, &term.expr) {
                evaluated.push(expr);
            }
        }
        // The expressions each GROUP BY term groups by: a result column's,
        // by its position or alias, or its own.
        let grouped: Vec<&Expr<'_>> = group_by
            .iter()
            .flat_map(|terms| terms.iter())
            .filter_map(|term| match order_term(columns, term) {
                OrderTerm::Position(expr) | OrderTerm::Alias(_, expr) | OrderTerm::Expr(expr) => {
                    Some(crate::sql::unparenthesized(expr))
                }
                OrderTerm::Unknown => None,
            })
            .collect();
        let inside = |expr: &Expr<'_>| {
            grouped.contains(&crate::sql::unparenthesized(expr))
                || aggregate_call(expr, self.telling.functions).is_some()
                || window_call(expr).is_some()
        };
        let mut outside = Vec::new();
        let mut stopped = Vec::new();
        let mut aggregated = group_by.is_some();
        for expr in evaluated {
            parts_outside(expr, &inside, &mut outside, &mut stopped);
            // An aggregate inside a window function's arguments makes the
            // core aggregate too.
            let mut found = Vec::new();
            parts(expr, &mut found);
            aggregated |= found.iter().any(|part| part.aggregates(self.telling.functions));
        }
        // The GROUP BY terms a result column, the HAVING or the ORDER BY
        // shows.
        let shown: Vec<&Expr<'_>> = stopped
            .iter()
            .copied()
            .filter(|expr| grouped.contains(&crate::sql::unparenthesized(expr)))
            .collect();
        let windows: Vec<&Over<'_>> = stopped.iter().filter_map(|expr| window_call(expr)).collect();
        let calls: Vec<AggregateCall<'_>> = stopped
            .into_iter()
            .filter_map(|expr| aggregate_call(expr, self.telling.functions))
            .collect();
        let items = self.telling.items(scope, &self.outer, from.as_ref())?;
        // A window numbers or reads the rows of each partition in the order
        // its ORDER BY sorts them, and among those that sort the same in the
        // order SQLite reads them; in a core that aggregates, it reads the
        // groups in the order SQLite makes them.
        let definitions = window_clause.unwrap_or_default();
        for over in windows {
            let order = window_order(over, definitions);
            let (false, Some(order)) = (aggregated, order) else { return Ok(true) };
            if !self.telling.core_tells_apart(scope, &self.outer, core, order)? {
                return Ok(true);
            }
        }
        if !aggregated {
            return Ok(false);
        }
        let bare = outside
            .iter()
            .any(|part| matches!(part, Part::Name(_) | Part::Select(_) | Part::Subquery(_)));
        if star || bare {
            return Ok(true);
        }
        // A GROUP BY term keeps the value it reads first among values that
        // compare equal.
        for term in shown {
            let Some(items) = &items else { return Ok(true) };
            if !self.compares_exactly(items, term)? {
                return Ok(true);
            }
        }
        for call in calls {
            match (call.name.as_str(), call.arguments, &items) {
                // count gives the same value in every order.
                ("count", ..) => continue,
                // min and max keep the value they read first among values
                // that compare equal.
                ("min" | "max", [argument], Some(items))
                    if self.compares_exactly(items, argument)? =>
                {
                    continue
                }
                ("min" | "max", ..) => return Ok(true),
                _ => {}
            }
            if !self.telling.core_tells_apart(scope, &self.outer, core, call.order)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A table's columns, lowercase.
    fn columns(&self, table: &str) -> rusqlite::Result<Vec<String>> {
        Ok(self
            .telling
            .schema
            .columns(table)?
            .iter()
            .map(|column| column.name.to_ascii_lowercase())
            .collect())
    }
}

/// The ORDER BY terms of the window `over`, among the windows `definitions`
/// its core's WINDOW clause names: its own, or else its base window's, or
/// none. `None` when a name names no window, which SQLite refuses.
fn window_order<'a>(
    over: &'a Over<'a>,
    definitions: &'a [WindowDef<'a>],
) -> Option<&'a [SortedColumn<'a>]> {
    let named = |name: &Name<'_>| {
        definitions.iter().find(|definition| definition.name == *name).map(|found| &found.window)
    };
    let mut window = match over {
        Over::Window(window) => *window,
        Over::Name(name) => named(name)?,
    };
    // Each base is another of the definitions, so a chain longer than them
    // repeats one.
    for _ in 0..=definitions.len() {
        if let Some(order) = window.order_by {
            return Some(order);
        }
        let Some(base) = &window.base else { return Some(&[]) };
        window = named(base)?;
    }
    None
}

/// The names, lowercase, SQLite gives the result columns of `select`'s first
/// arm, when each is an alias or a column's name; `None` when one is an
/// expression or a `*`.
fn result_names(select: &Select<'_>) -> Option<Vec<String>> {
    let OneSelect::Select { columns, .. } = &select.body.select else {
        let OneSelect::Values(rows) = &select.body.select else { return None };
        return Some(crate::sql::values_names(
            rows.first().expect("a parsed VALUES has at least one row").len(),
        ));
    };
    columns
        .iter()
        .map(|column| {
            let name = match column {
                ResultColumn::Expr(_, Some(alias)) => return Some(crate::sql::alias_name(alias)),
                ResultColumn::Expr(expr, None) => crate::sql::column_name(expr)?,
                ResultColumn::Star | ResultColumn::TableStar(_) => return None,
            };
            Some(crate::sql::name(name).to_ascii_lowercase())
        })
        .collect()
}

#[cfg(test)]
#[path = "unreported_columns_tests.rs"]
mod tests;
