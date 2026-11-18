//! The columns a USING or NATURAL join compares, which SQLite's authorizer
//! doesn't report as read though the join's rows depend on them (§4). A
//! statement's reads add them for each table the join compares directly, in
//! any SELECT of the statement and in any of the app's views it reads. A view's
//! or a subquery's columns the join compares are named in its own SELECT,
//! where the authorizer reports the columns of its tables itself. When the
//! parser can't read the statement or a view it reads, the columns are
//! unknown, and every column of every table counts as read.

use std::collections::BTreeSet;

use crate::read_set::ColumnSet;
use crate::schema_objects::{declared_columns, main_object, read_view, MAIN_TABLES};
use crate::select_syntax::{parts, window_parts, Part};
use rusqlite::Connection;
use sqlite3_parser::ast::{
    Expr, FromClause, JoinConstraint, JoinOperator, JoinType, OneSelect, QualifiedName,
    ResultColumn, Select, SelectTable,
};

/// The columns a statement's USING and NATURAL joins compare, as (table,
/// column) pairs, lowercase.
#[derive(Clone)]
pub(crate) enum JoinedColumns {
    Known(ColumnSet),
    /// The parser couldn't read the statement or a view it reads: every
    /// column of every table in the main schema (`MAIN_TABLES`), so every
    /// write reruns the statement.
    Unknown(ColumnSet),
}

impl JoinedColumns {
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
            "the planner can't tell which columns a statement's joins compare, so every \
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

/// The columns USING and NATURAL joins compare in `select`.
pub(crate) fn joined_columns(
    connection: &Connection,
    select: &Select<'_>,
) -> rusqlite::Result<JoinedColumns> {
    let mut walk = Walk {
        connection,
        views: BTreeSet::new(),
        ctes: Vec::new(),
        found: ColumnSet::new(),
        unread: None,
    };
    walk.select(select)?;
    match walk.unread {
        Some(why) => JoinedColumns::unknown(connection, why),
        None => Ok(JoinedColumns::Known(walk.found)),
    }
}

struct Walk<'c> {
    connection: &'c Connection,
    /// The views read so far, lowercase, so a view read twice is read once.
    views: BTreeSet<String>,
    /// The names, lowercase, of the CTEs where the walk is, which a FROM
    /// item's name means before any table or view of that name.
    ctes: Vec<String>,
    found: ColumnSet,
    /// Why the parser couldn't read a view the statement reads.
    unread: Option<String>,
}

impl Walk<'_> {
    fn select(&mut self, select: &Select<'_>) -> rusqlite::Result<()> {
        let outer = self.ctes.len();
        let ctes = select.with.iter().flat_map(|with| with.ctes.iter());
        self.ctes
            .extend(ctes.clone().map(|cte| crate::sql::name(cte.tbl_name.0).to_ascii_lowercase()));
        for cte in ctes {
            self.select(cte.select)?;
        }
        let arms = std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().flatten().map(|compound| &compound.select));
        for arm in arms {
            self.core(arm)?;
        }
        for term in select.order_by.iter().flat_map(|terms| terms.iter()) {
            self.expr(&term.expr)?;
        }
        if let Some(limit) = select.limit {
            self.expr(&limit.expr)?;
            if let Some(offset) = &limit.offset {
                self.expr(offset)?;
            }
        }
        self.ctes.truncate(outer);
        Ok(())
    }

    fn core(&mut self, core: &OneSelect<'_>) -> rusqlite::Result<()> {
        match core {
            OneSelect::Values(rows) => {
                for expr in rows.iter().flat_map(|row| row.iter()) {
                    self.expr(expr)?;
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
                        self.expr(expr)?;
                    }
                }
                if let Some(from) = from {
                    self.from(from)?;
                }
                for expr in where_clause.iter().copied().chain(having.iter().copied()) {
                    self.expr(expr)?;
                }
                for expr in group_by.iter().flat_map(|exprs| exprs.iter()) {
                    self.expr(expr)?;
                }
                let mut found = Vec::new();
                for definition in window_clause.iter().flat_map(|windows| windows.iter()) {
                    window_parts(&definition.window, &mut found);
                }
                self.selects(&found)?;
            }
        }
        Ok(())
    }

    /// Each FROM item, then each USING or NATURAL join's columns: a USING
    /// column of the joined table and of the leftmost item before the join
    /// that has it, which SQLite compares it with, and every column of each
    /// table before the join or joined for a NATURAL join, which compares the
    /// columns the two sides share.
    fn from(&mut self, from: &FromClause<'_>) -> rusqlite::Result<()> {
        let items: Vec<&SelectTable<'_>> = from
            .select
            .into_iter()
            .chain(from.joins.iter().flatten().map(|join| &join.table))
            .collect();
        for item in &items {
            self.item(item)?;
        }
        for (position, join) in from.joins.iter().flatten().enumerate() {
            if let Some(JoinConstraint::On(expr)) = &join.constraint {
                self.expr(expr)?;
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
        Ok(())
    }

    /// The table `item` reads directly, and its columns, lowercase.
    fn table(&self, item: &SelectTable<'_>) -> rusqlite::Result<Option<(String, Vec<String>)>> {
        let SelectTable::Table(name, ..) = item else { return Ok(None) };
        let Some(table) = self.object(name, "table")? else { return Ok(None) };
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
                let Some(object) = self.object(name, "table")?.or(self.object(name, "view")?)
                else {
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

    fn item(&mut self, item: &SelectTable<'_>) -> rusqlite::Result<()> {
        match item {
            SelectTable::Table(name, ..) => {
                let Some(view) = self.object(name, "view")? else { return Ok(()) };
                if !self.views.insert(view.to_ascii_lowercase()) {
                    return Ok(());
                }
                let connection = self.connection;
                // A view's SELECT names no CTE of the statement.
                let ctes = std::mem::take(&mut self.ctes);
                let read = read_view(connection, &view, |_, select| self.select(select));
                self.ctes = ctes;
                if let Err(why) = read? {
                    self.unread.get_or_insert(why);
                }
                Ok(())
            }
            SelectTable::TableCall(_, args, _) => {
                for arg in args.iter().flat_map(|args| args.iter()) {
                    self.expr(arg)?;
                }
                Ok(())
            }
            SelectTable::Select(select, _) => self.select(select),
            SelectTable::Sub(from, _) => self.from(from),
        }
    }

    fn expr(&mut self, expr: &Expr<'_>) -> rusqlite::Result<()> {
        let mut found = Vec::new();
        parts(expr, &mut found);
        self.selects(&found)
    }

    fn selects(&mut self, found: &[Part<'_>]) -> rusqlite::Result<()> {
        for part in found {
            if let Part::Select(select) = part {
                self.select(select)?;
            }
        }
        Ok(())
    }

    /// The stored name of the main schema's object of type `kind`, `table`
    /// or `view`, that `name` names. A name of another schema reads no table
    /// of the app's, and a CTE's name none of the schema's objects.
    fn object(&self, name: &QualifiedName<'_>, kind: &str) -> rusqlite::Result<Option<String>> {
        if name.db_name.as_ref().is_some_and(|schema| *schema != "main") {
            return Ok(None);
        }
        if name.db_name.is_none() && self.ctes.iter().any(|cte| name.name == cte.as_str()) {
            return Ok(None);
        }
        Ok(main_object(self.connection, &name.name)?
            .filter(|(_, found)| found == kind)
            .map(|(stored, _)| stored))
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
        return Some(crate::sql::values_names(rows.first().map_or(0, |row| row.len())));
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
#[path = "joined_columns_tests.rs"]
mod tests;
