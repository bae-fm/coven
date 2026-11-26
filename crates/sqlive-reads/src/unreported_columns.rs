//! The columns a statement's rows depend on that SQLite's authorizer doesn't
//! report as read (§4). There are two kinds.
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
//! names can decide which rows those are. An ORDER BY that sorts by the rowid
//! of each of the query's tables tells every two of its rows apart, so such a
//! query depends only on the columns it names, as does one that aggregates
//! with no GROUP BY, which returns one row. The tables a query reads are
//! those of its FROM, of the views and CTEs it reads, and of every query inside
//! it. The model states the rule as `Statement.picks` and proves it in
//! `crates/sqlive/proofs/SqliveNarrowing/Rules/Pick.lean`.
//!
//! A core that aggregates or calls a window function picks rows too, so every
//! column of each table it reads counts as read when its rows follow the
//! order SQLite reads its rows in. A window function numbers or reads the
//! rows of a partition in that order among those its ORDER BY sorts the
//! same, unless that ORDER BY sorts by the rowid of each of the core's
//! tables in a core that doesn't aggregate. A result column, the HAVING or
//! an ORDER BY term that names a column
//! outside every aggregate call and GROUP BY term takes the column's value
//! from one row of its group, which that order decides. A GROUP BY term, and
//! min and max, keep the value they read first among values that compare
//! equal, so a result column, the HAVING or an ORDER BY term that shows the
//! term follows that order, and so does a call of min or max, unless the term
//! or the argument is a column whose equal values are the same, and any other
//! aggregate but count reads its values in that order, as `group_concat`
//! joins them, unless the call's own ORDER BY sorts by the rowid of each of
//! the core's tables. The model states the rule as `Statement.followsOrder`.
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
use crate::schema_objects::{declared_columns, MAIN_TABLES, ROWID_NAMES};
use crate::scope_term::Affinity;
use crate::select_syntax::{
    aggregate_call, order_term, parts, parts_outside, window_call, AggregateCall, Functions,
    OrderTerm, Part,
};
use crate::sql_walk::{Reads, Scope, Visit, Walk, Within};
use rusqlite::Connection;
use sqlite3_parser::ast::{
    As, CompoundOperator, Distinctness, Expr, FromClause, JoinConstraint, JoinOperator, JoinType,
    Name, OneSelect, Over, ResultColumn, Select, SelectTable, SortedColumn, WindowDef,
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
    pub(crate) fn unknown(connection: &Connection, why: String) -> rusqlite::Result<Self> {
        tracing::warn!(
            %why,
            "the planner can't tell which columns a statement's rows depend on, so every \
             column of every table counts as read"
        );
        let mut tables = connection.prepare_cached(MAIN_TABLES)?;
        let tables = tables
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut every = ColumnSet::new();
        for table in tables {
            for column in declared_columns(connection, &table)? {
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
    functions: &Functions,
    select: &Select<'_>,
) -> rusqlite::Result<UnreportedColumns> {
    let found = Found { connection, functions, picked: Vec::new(), found: Vec::new() };
    let mut walk = Walk::new(connection, found);
    let reads = walk.select(select, false)?;
    if let Some(why) = walk.scope.unread() {
        return UnreportedColumns::unknown(connection, why.to_owned());
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
    connection: &'c Connection,
    /// The connection's functions, which tell an aggregate call from a
    /// scalar one.
    functions: &'c Functions,
    /// What each query whose rows SQLite picks reads, and where it is.
    picked: Vec<(Option<Within>, Reads)>,
    /// The columns USING and NATURAL joins compare, and where each join is.
    found: Vec<(Option<Within>, (String, String))>,
}

impl Visit for Found<'_> {
    /// Note `select` when SQLite picks its rows.
    fn select(
        &mut self,
        scope: &Scope<'_>,
        select: &Select<'_>,
        first_row: bool,
        reads: &Reads,
    ) -> rusqlite::Result<()> {
        if (select.limit.is_some() || first_row) && !self.fully_orders(scope, select)?
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
        let Some(table) = scope.object(name, &["table", "virtual"])? else { return Ok(None) };
        let columns = self.columns(&table)?;
        Ok(Some((table, columns)))
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
                let Some(object) = scope.object(name, &["table", "view", "virtual"])? else {
                    return Ok(None);
                };
                self.columns(&object)?
            }
            SelectTable::Select(select, _) => {
                let Some(names) = result_names(select) else { return Ok(None) };
                names
            }
            SelectTable::TableCall(..) | SelectTable::Sub(..) => return Ok(None),
        };
        Ok(Some(names.iter().any(|found| found == column)))
    }

    /// Whether `select`'s ORDER BY tells every two of its rows apart. The
    /// query is one SELECT, and either it calls an aggregate function in its
    /// result columns with no GROUP BY and no window, so it returns one row,
    /// or it has no DISTINCT, neither groups nor calls an aggregate or window
    /// function, each of its FROM items is a table of the main schema, and
    /// its ORDER BY sorts by the rowid of each one.
    fn fully_orders(&self, scope: &Scope<'_>, select: &Select<'_>) -> rusqlite::Result<bool> {
        if select.body.compounds.is_some() {
            return Ok(false);
        }
        let OneSelect::Select {
            distinctness,
            columns,
            from,
            group_by: None,
            having,
            window_clause: None,
            ..
        } = &select.body.select
        else {
            return Ok(false);
        };
        let order_by = select.order_by.unwrap_or_default();
        let mut returned = Vec::new();
        for column in columns.iter() {
            if let ResultColumn::Expr(expr, _) = column {
                parts(expr, &mut returned);
            }
        }
        let mut sorted = Vec::new();
        for term in order_by {
            parts(&term.expr, &mut sorted);
        }
        let windowed = returned.iter().chain(&sorted).any(Part::windowed);
        if !windowed && returned.iter().any(|part| part.aggregates(self.functions)) {
            return Ok(true);
        }
        if windowed
            || distinctness.is_some()
            || having.is_some()
            || sorted.iter().any(|part| part.aggregates(self.functions))
        {
            return Ok(false);
        }
        let Some(items) = self.table_items(scope, from.as_ref())? else { return Ok(false) };
        let mut sorted = Vec::new();
        for term in order_by {
            sorted.push(match order_term(columns, &term.expr) {
                OrderTerm::Position(expr) | OrderTerm::Alias(_, expr) | OrderTerm::Expr(expr) => {
                    self.item_column(&items, expr)?
                }
                OrderTerm::Unknown => None,
            });
        }
        self.sorts_by_rowids(&items, &sorted)
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
    /// are the same (`compares_exactly`) of one of its FROM items, each a
    /// table.
    fn returns_exactly(&self, scope: &Scope<'_>, core: &OneSelect<'_>) -> rusqlite::Result<bool> {
        let OneSelect::Select { columns, from, .. } = core else { return Ok(false) };
        let Some(items) = self.table_items(scope, from.as_ref())? else { return Ok(false) };
        for column in columns.iter() {
            let ResultColumn::Expr(expr, _) = column else { return Ok(false) };
            if !self.compares_exactly(&items, expr)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Each FROM item of `from`, by the name the statement qualifies its
    /// columns by, with its table, when each is a table of the main schema.
    fn table_items<'a>(
        &self,
        scope: &Scope<'_>,
        from: Option<&'a FromClause<'a>>,
    ) -> rusqlite::Result<Option<Vec<(&'a str, String)>>> {
        let mut items = Vec::new();
        let listed = from.into_iter().flat_map(|from| {
            from.select.into_iter().chain(from.joins.iter().flatten().map(|join| &join.table))
        });
        for item in listed {
            let SelectTable::Table(name, alias, _) = item else { return Ok(None) };
            // A CTE's name names no table (`main`).
            let Some(table) = scope.object(name, &["table", "virtual"])? else { return Ok(None) };
            let qualifier = match alias {
                Some(As::As(alias) | As::Elided(alias)) => alias.0,
                None => name.name.0,
            };
            items.push((qualifier, table));
        }
        Ok(Some(items))
    }

    /// Whether a sort by `sorted`, the FROM item and column each term
    /// names among `items` (`item_column`), sorts by the rowid of each of
    /// `items`, and so tells every two of their rows apart.
    fn sorts_by_rowids(
        &self,
        items: &[(&str, String)],
        sorted: &[Option<(usize, String)>],
    ) -> rusqlite::Result<bool> {
        for (position, (_, table)) in items.iter().enumerate() {
            let mut found = false;
            for (item, column) in sorted.iter().flatten() {
                if *item == position && self.is_rowid(table, column)? {
                    found = true;
                    break;
                }
            }
            if !found {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether the rows of a core follow the order SQLite reads its rows in.
    /// A window function numbers or reads the rows of a partition in that
    /// order among those its ORDER BY sorts the same, unless the ORDER BY
    /// sorts by the rowid of each of the core's FROM items, each a table, in
    /// a core that doesn't aggregate. In a core that aggregates, a result
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
    /// of reals adds them, unless its own ORDER BY sorts by the rowid of each
    /// of the core's FROM items, each a table.
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
                || aggregate_call(expr, self.functions).is_some()
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
            aggregated |= found.iter().any(|part| part.aggregates(self.functions));
        }
        // The GROUP BY terms a result column, the HAVING or the ORDER BY
        // shows.
        let shown: Vec<&Expr<'_>> = stopped
            .iter()
            .copied()
            .filter(|expr| grouped.contains(&crate::sql::unparenthesized(expr)))
            .collect();
        let windows: Vec<&Over<'_>> = stopped.iter().filter_map(|expr| window_call(expr)).collect();
        let calls: Vec<AggregateCall<'_>> =
            stopped.into_iter().filter_map(|expr| aggregate_call(expr, self.functions)).collect();
        let items = self.table_items(scope, from.as_ref())?;
        // A window numbers or reads the rows of each partition in the order
        // its ORDER BY sorts them, and among those that sort the same in the
        // order SQLite reads them; in a core that aggregates, it reads the
        // groups in the order SQLite makes them.
        let definitions = window_clause.unwrap_or_default();
        for over in windows {
            let order = window_order(over, definitions);
            let (false, Some(order), Some(items)) = (aggregated, order, &items) else {
                return Ok(true);
            };
            let mut sorted = Vec::new();
            for term in order {
                sorted.push(self.item_column(items, &term.expr)?);
            }
            if !self.sorts_by_rowids(items, &sorted)? {
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
            let Some(items) = &items else { return Ok(true) };
            let mut sorted = Vec::new();
            for term in call.order {
                sorted.push(self.item_column(items, &term.expr)?);
            }
            if !self.sorts_by_rowids(items, &sorted)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether two values that the column `expr` names among `items` holds
    /// that compare equal are the same value: a rowid, or a stored column of
    /// BINARY collation whose declared type gives it an affinity other than
    /// BLOB, which stores an integer and a real that compare equal as one of
    /// them. An untyped column can hold both `1` and `1.0`, and a NOCASE one
    /// both `'Abc'` and `'ABC'`.
    fn compares_exactly(
        &self,
        items: &[(&str, String)],
        expr: &Expr<'_>,
    ) -> rusqlite::Result<bool> {
        let Some((item, column)) = self.item_column(items, expr)? else { return Ok(false) };
        let table = &items[item].1;
        if self.is_rowid(table, &column)? {
            return Ok(true);
        }
        let columns =
            crate::from_item::columns(self.connection, self.functions, items[item].0, table, true)?;
        Ok(matches!(columns.affinity(&column), Some(Affinity::Numeric | Affinity::Text)))
    }

    /// The FROM item, by its place in `items`, and the column, lowercase,
    /// that the name `expr` names, as SQLite resolves it among `items`.
    fn item_column(
        &self,
        items: &[(&str, String)],
        expr: &Expr<'_>,
    ) -> rusqlite::Result<Option<(usize, String)>> {
        let (qualifier, name) = match crate::sql::unparenthesized(expr) {
            Expr::Id(name) => (None, name.0),
            Expr::Qualified(table, name) => (Some(table), name.0),
            Expr::DoublyQualified(db, table, name) if *db == "main" => (Some(table), name.0),
            _ => return Ok(None),
        };
        let column = crate::sql::name(name).to_ascii_lowercase();
        if let Some(qualifier) = qualifier {
            let found = items.iter().position(|(named, _)| Name(named) == *qualifier);
            return Ok(found.map(|item| (item, column)));
        }
        let mut having = Vec::new();
        for (item, (_, table)) in items.iter().enumerate() {
            if self.columns(table)?.contains(&column) {
                having.push(item);
            }
        }
        Ok(match having[..] {
            [item] => Some((item, column)),
            // A rowid's name that no column has names the rowid of the one
            // table.
            [] if items.len() == 1 && ROWID_NAMES.contains(&column.as_str()) => Some((0, column)),
            _ => None,
        })
    }

    /// Whether `column`, lowercase, names the rowid of `table`, a table of
    /// the main schema: its INTEGER PRIMARY KEY column, or `rowid`, `oid` or
    /// `_rowid_` where no column has that name. A WITHOUT ROWID table has no
    /// rowid.
    fn is_rowid(&self, table: &str, column: &str) -> rusqlite::Result<bool> {
        let without_rowid: bool = self
            .connection
            .prepare_cached("SELECT wr FROM pragma_table_list WHERE schema='main' AND name=?1")?
            .query_row([table], |row| row.get(0))?;
        if without_rowid {
            return Ok(false);
        }
        if ROWID_NAMES.contains(&column) {
            return Ok(!self.columns(table)?.iter().any(|found| found == column));
        }
        // A rowid table's single primary key column holds the rowid exactly
        // when SQLite keeps no index for the key: `INTEGER PRIMARY KEY`, but
        // not `INT PRIMARY KEY` or `INTEGER PRIMARY KEY DESC`.
        let keys = self
            .connection
            .prepare_cached("SELECT name FROM pragma_table_info(?1,'main') WHERE pk > 0")?
            .query_map([table], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let indexed: bool = self
            .connection
            .prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM pragma_index_list(?1,'main') WHERE origin='pk')",
            )?
            .query_row([table], |row| row.get(0))?;
        Ok(!indexed && matches!(&keys[..], [key] if key.eq_ignore_ascii_case(column)))
    }

    /// A table's columns, lowercase.
    fn columns(&self, table: &str) -> rusqlite::Result<Vec<String>> {
        Ok(declared_columns(self.connection, table)?
            .into_iter()
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
                ResultColumn::Expr(expr, None) => match crate::sql::unparenthesized(expr) {
                    Expr::Id(name) => name.0,
                    Expr::Qualified(_, name) | Expr::DoublyQualified(_, _, name) => name.0,
                    _ => return None,
                },
                ResultColumn::Star | ResultColumn::TableStar(_) => return None,
            };
            Some(crate::sql::name(name).to_ascii_lowercase())
        })
        .collect()
}

#[cfg(test)]
#[path = "unreported_columns_tests.rs"]
mod tests;
