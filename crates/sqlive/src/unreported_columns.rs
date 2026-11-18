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
//! When the parser can't read the statement or a view it reads, the columns
//! are unknown, and every column of every table counts as read.

use std::collections::{BTreeMap, BTreeSet};

use crate::read_set::ColumnSet;
use crate::schema_objects::{declared_columns, main_object, read_view, MAIN_TABLES, ROWID_NAMES};
use crate::select_syntax::{order_term, parts, window_parts, OrderTerm, Part};
use rusqlite::Connection;
use sqlite3_parser::ast::{
    As, Expr, FromClause, JoinConstraint, JoinOperator, JoinType, Name, OneSelect, QualifiedName,
    ResultColumn, Select, SelectTable,
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

/// The columns `select`'s rows depend on that the authorizer doesn't report;
/// `aggregates` names the connection's aggregate and window functions,
/// lowercase.
pub(crate) fn unreported_columns(
    connection: &Connection,
    aggregates: &BTreeSet<String>,
    select: &Select<'_>,
) -> rusqlite::Result<UnreportedColumns> {
    let mut walk = Walk {
        connection,
        aggregates,
        views: BTreeMap::new(),
        ctes: Vec::new(),
        defined: Vec::new(),
        picked: Vec::new(),
        found: ColumnSet::new(),
        unread: None,
    };
    walk.select(select, false)?;
    if let Some(why) = walk.unread {
        return UnreportedColumns::unknown(connection, why);
    }
    let mut picked = BTreeSet::new();
    for reads in &walk.picked {
        picked.extend(walk.tables(reads));
    }
    for table in picked {
        for column in walk.columns(&table)? {
            walk.found.insert((table.to_ascii_lowercase(), column));
        }
    }
    Ok(UnreportedColumns::Known(walk.found))
}

/// The tables a query reads, in its FROM, in the views it reads and in the
/// queries inside it, and the CTEs it reads, whose own tables count too.
#[derive(Clone, Default)]
struct Reads {
    /// Tables of the main schema, by their stored names.
    tables: BTreeSet<String>,
    /// CTEs, by their places in `Walk::defined`.
    ctes: BTreeSet<usize>,
}

impl Reads {
    fn extend(&mut self, other: Self) {
        self.tables.extend(other.tables);
        self.ctes.extend(other.ctes);
    }
}

struct Walk<'c> {
    connection: &'c Connection,
    /// The connection's aggregate and window functions, lowercase.
    aggregates: &'c BTreeSet<String>,
    /// What each view read so far reads, by its name, lowercase, so a view
    /// read twice is read once.
    views: BTreeMap<String, Reads>,
    /// The CTEs where the walk is: each one's name, lowercase, which a FROM
    /// item's name means before any table or view of that name, and its place
    /// in `defined`.
    ctes: Vec<(String, usize)>,
    /// What each CTE the walk has met reads. A CTE can name another defined
    /// after it in the same WITH, so the walk reads every CTE's SELECT before
    /// it follows the CTEs a query names.
    defined: Vec<Reads>,
    /// What each query whose rows SQLite picks reads.
    picked: Vec<Reads>,
    found: ColumnSet,
    /// Why the parser couldn't read a view the statement reads.
    unread: Option<String>,
}

impl Walk<'_> {
    /// Read `select`, a subquery whose first row an expression takes when
    /// `first_row`, and note it when SQLite picks its rows.
    fn select(&mut self, select: &Select<'_>, first_row: bool) -> rusqlite::Result<Reads> {
        let outer = self.ctes.len();
        let ctes = select.with.iter().flat_map(|with| with.ctes.iter());
        for cte in ctes.clone() {
            let name = crate::sql::name(cte.tbl_name.0).to_ascii_lowercase();
            self.ctes.push((name, self.defined.len()));
            self.defined.push(Reads::default());
        }
        for (position, cte) in ctes.enumerate() {
            let reads = self.select(cte.select, false)?;
            let place = self.ctes[outer + position].1;
            self.defined[place] = reads;
        }
        let mut reads = Reads::default();
        let arms = std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().flatten().map(|compound| &compound.select));
        for arm in arms {
            reads.extend(self.core(arm)?);
        }
        for term in select.order_by.iter().flat_map(|terms| terms.iter()) {
            reads.extend(self.expr(&term.expr)?);
        }
        if let Some(limit) = select.limit {
            reads.extend(self.expr(&limit.expr)?);
            if let Some(offset) = &limit.offset {
                reads.extend(self.expr(offset)?);
            }
        }
        if (select.limit.is_some() || first_row) && !self.fully_orders(select)? {
            self.picked.push(reads.clone());
        }
        self.ctes.truncate(outer);
        Ok(reads)
    }

    fn core(&mut self, core: &OneSelect<'_>) -> rusqlite::Result<Reads> {
        let mut reads = Reads::default();
        match core {
            OneSelect::Values(rows) => {
                for expr in rows.iter().flat_map(|row| row.iter()) {
                    reads.extend(self.expr(expr)?);
                }
            }
            OneSelect::Select {
                columns,
                from,
                where_clause,
                group_by,
                having,
                window_clause,
                ..
            } => {
                for column in columns.iter() {
                    if let ResultColumn::Expr(expr, _) = column {
                        reads.extend(self.expr(expr)?);
                    }
                }
                if let Some(from) = from {
                    reads.extend(self.from(from)?);
                }
                for expr in where_clause.iter().copied().chain(having.iter().copied()) {
                    reads.extend(self.expr(expr)?);
                }
                for expr in group_by.iter().flat_map(|exprs| exprs.iter()) {
                    reads.extend(self.expr(expr)?);
                }
                let mut found = Vec::new();
                for definition in window_clause.iter().flat_map(|windows| windows.iter()) {
                    window_parts(&definition.window, &mut found);
                }
                reads.extend(self.selects(&found)?);
            }
        }
        Ok(reads)
    }

    /// Each FROM item, then each USING or NATURAL join's columns: a USING
    /// column of the joined table and of the leftmost item before the join
    /// that has it, which SQLite compares it with, and every column of each
    /// table before the join or joined for a NATURAL join, which compares the
    /// columns the two sides share.
    fn from(&mut self, from: &FromClause<'_>) -> rusqlite::Result<Reads> {
        let items: Vec<&SelectTable<'_>> = from
            .select
            .into_iter()
            .chain(from.joins.iter().flatten().map(|join| &join.table))
            .collect();
        let mut reads = Reads::default();
        for item in &items {
            reads.extend(self.item(item)?);
        }
        for (position, join) in from.joins.iter().flatten().enumerate() {
            if let Some(JoinConstraint::On(expr)) = &join.constraint {
                reads.extend(self.expr(expr)?);
            }
            // The items before the join's own, which sits after the first.
            let (before, joined) = (&items[..=position], items[position + 1]);
            if matches!(
                join.operator,
                JoinOperator::TypedJoin(Some(kind)) if kind.contains(JoinType::NATURAL)
            ) {
                for item in before.iter().chain([&joined]) {
                    if let Some((table, columns)) = self.table(item)? {
                        for column in columns {
                            self.found.insert((table.to_ascii_lowercase(), column));
                        }
                    }
                }
                continue;
            }
            let Some(JoinConstraint::Using(names)) = &join.constraint else { continue };
            for name in names.iter() {
                let column = crate::sql::name(name.0).to_ascii_lowercase();
                self.table_column(joined, &column)?;
                // Past an item whose columns this doesn't know, any table
                // after it could be the one compared.
                let mut known = true;
                for item in before {
                    match self.has_column(item, &column)? {
                        Some(false) => {}
                        Some(true) => {
                            self.table_column(item, &column)?;
                            if known {
                                break;
                            }
                        }
                        None => known = false,
                    }
                }
            }
        }
        Ok(reads)
    }

    /// The table `item` reads directly, and its columns, lowercase.
    fn table(&self, item: &SelectTable<'_>) -> rusqlite::Result<Option<(String, Vec<String>)>> {
        let SelectTable::Table(name, ..) = item else { return Ok(None) };
        let Some(table) = self.object(name, &["table"])? else { return Ok(None) };
        let columns = self.columns(&table)?;
        Ok(Some((table, columns)))
    }

    /// Count `column` of the table `item` reads directly, when it has one.
    fn table_column(&mut self, item: &SelectTable<'_>, column: &str) -> rusqlite::Result<()> {
        if let Some((table, columns)) = self.table(item)? {
            if columns.iter().any(|found| found == column) {
                self.found.insert((table.to_ascii_lowercase(), column.to_owned()));
            }
        }
        Ok(())
    }

    /// Whether `item` has a column named `column`, when this knows: a table
    /// or a view of the main schema, or a subquery whose result columns
    /// SQLite names by their aliases or the columns they name.
    fn has_column(&self, item: &SelectTable<'_>, column: &str) -> rusqlite::Result<Option<bool>> {
        let names = match item {
            SelectTable::Table(name, ..) => {
                let Some(object) = self.object(name, &["table", "view"])? else {
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

    fn item(&mut self, item: &SelectTable<'_>) -> rusqlite::Result<Reads> {
        match item {
            SelectTable::Table(name, ..) => {
                if let Some(place) = self.cte(name) {
                    return Ok(Reads { ctes: BTreeSet::from([place]), ..Reads::default() });
                }
                let view = match self.main(name)? {
                    Some((table, kind)) if kind == "table" => {
                        return Ok(Reads { tables: BTreeSet::from([table]), ..Reads::default() });
                    }
                    Some((view, _)) => view,
                    None => return Ok(Reads::default()),
                };
                if let Some(reads) = self.views.get(&view.to_ascii_lowercase()) {
                    return Ok(reads.clone());
                }
                let connection = self.connection;
                // A view's SELECT names no CTE of the statement.
                let ctes = std::mem::take(&mut self.ctes);
                let read = read_view(connection, &view, |_, select| self.select(select, false));
                self.ctes = ctes;
                match read? {
                    Ok(reads) => {
                        self.views.insert(view.to_ascii_lowercase(), reads.clone());
                        Ok(reads)
                    }
                    Err(why) => {
                        self.unread.get_or_insert(why);
                        Ok(Reads::default())
                    }
                }
            }
            SelectTable::TableCall(_, args, _) => {
                let mut reads = Reads::default();
                for arg in args.iter().flat_map(|args| args.iter()) {
                    reads.extend(self.expr(arg)?);
                }
                Ok(reads)
            }
            SelectTable::Select(select, _) => self.select(select, false),
            SelectTable::Sub(from, _) => self.from(from),
        }
    }

    fn expr(&mut self, expr: &Expr<'_>) -> rusqlite::Result<Reads> {
        let mut found = Vec::new();
        parts(expr, &mut found);
        self.selects(&found)
    }

    fn selects(&mut self, found: &[Part<'_>]) -> rusqlite::Result<Reads> {
        let mut reads = Reads::default();
        for part in found {
            match part {
                Part::Select(select) => reads.extend(self.select(select, false)?),
                Part::Subquery(select) => reads.extend(self.select(select, true)?),
                _ => {}
            }
        }
        Ok(reads)
    }

    /// The place in `defined` of the CTE that `name` names where the walk
    /// is, the innermost of that name.
    fn cte(&self, name: &QualifiedName<'_>) -> Option<usize> {
        if name.db_name.is_some() {
            return None;
        }
        self.ctes.iter().rev().find(|(cte, _)| name.name == cte.as_str()).map(|(_, place)| *place)
    }

    /// The tables `reads` reads, with those of the CTEs it reads and of the
    /// CTEs those read.
    fn tables(&self, reads: &Reads) -> BTreeSet<String> {
        let mut tables = reads.tables.clone();
        let mut followed = BTreeSet::new();
        let mut waiting: Vec<usize> = reads.ctes.iter().copied().collect();
        while let Some(place) = waiting.pop() {
            if !followed.insert(place) {
                continue;
            }
            let cte = &self.defined[place];
            tables.extend(cte.tables.iter().cloned());
            waiting.extend(cte.ctes.iter().copied());
        }
        tables
    }

    /// Whether `select`'s ORDER BY tells every two of its rows apart. The
    /// query is one SELECT, and either it calls an aggregate function in its
    /// result columns with no GROUP BY and no window, so it returns one row,
    /// or it has no DISTINCT, neither groups nor calls an aggregate or window
    /// function, each of its FROM items is a table of the main schema, and
    /// its ORDER BY sorts by the rowid of each one.
    fn fully_orders(&self, select: &Select<'_>) -> rusqlite::Result<bool> {
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
        if !windowed && returned.iter().any(|part| part.aggregates(self.aggregates)) {
            return Ok(true);
        }
        if windowed
            || distinctness.is_some()
            || having.is_some()
            || sorted.iter().any(|part| part.aggregates(self.aggregates))
        {
            return Ok(false);
        }
        // Each FROM item: the name the statement qualifies its columns by,
        // and its table.
        let mut items = Vec::new();
        let listed = from.iter().flat_map(|from| {
            from.select.into_iter().chain(from.joins.iter().flatten().map(|join| &join.table))
        });
        for item in listed {
            let SelectTable::Table(name, alias, _) = item else { return Ok(false) };
            // A CTE's name names no table (`main`).
            let Some(table) = self.object(name, &["table"])? else { return Ok(false) };
            let qualifier = match alias {
                Some(As::As(alias) | As::Elided(alias)) => alias.0,
                None => name.name.0,
            };
            items.push((qualifier, table));
        }
        for (position, (_, table)) in items.iter().enumerate() {
            let mut sorted = false;
            for term in order_by {
                let sorted_by = match order_term(columns, &term.expr) {
                    OrderTerm::Position(expr)
                    | OrderTerm::Alias(_, expr)
                    | OrderTerm::Expr(expr) => self.item_column(&items, expr)?,
                    OrderTerm::Unknown => None,
                };
                if let Some((item, column)) = sorted_by {
                    if item == position && self.is_rowid(table, &column)? {
                        sorted = true;
                        break;
                    }
                }
            }
            if !sorted {
                return Ok(false);
            }
        }
        Ok(true)
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
            Expr::Name(name) => (None, name.0),
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

    /// The stored name of the main schema's object that `name` names, when
    /// its type is one of `kinds`, `table` or `view`.
    fn object(&self, name: &QualifiedName<'_>, kinds: &[&str]) -> rusqlite::Result<Option<String>> {
        Ok(self
            .main(name)?
            .filter(|(_, found)| kinds.contains(&found.as_str()))
            .map(|(stored, _)| stored))
    }

    /// The stored name and the type, `table` or `view`, of the main schema's
    /// object that `name` names. A name of another schema reads no table of
    /// the app's, and a CTE's name none of the schema's objects.
    fn main(&self, name: &QualifiedName<'_>) -> rusqlite::Result<Option<(String, String)>> {
        if name.db_name.as_ref().is_some_and(|schema| *schema != "main") {
            return Ok(None);
        }
        if self.cte(name).is_some() {
            return Ok(None);
        }
        main_object(self.connection, &name.name)
    }

    /// A table's columns, lowercase.
    fn columns(&self, table: &str) -> rusqlite::Result<Vec<String>> {
        Ok(declared_columns(self.connection, table)?
            .into_iter()
            .map(|column| column.name.to_ascii_lowercase())
            .collect())
    }
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
                    Expr::Name(name)
                    | Expr::Qualified(_, name)
                    | Expr::DoublyQualified(_, _, name) => name.0,
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
