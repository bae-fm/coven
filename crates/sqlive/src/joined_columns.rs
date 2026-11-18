//! The columns a USING or NATURAL join compares, which SQLite's authorizer
//! doesn't report as read though the join's rows depend on them (§4). A
//! statement's reads add them for each table the join reads directly, in any
//! SELECT of the statement and in any of the app's views it reads. A view's
//! or a subquery's columns the join compares are named in its own SELECT,
//! where the authorizer reports the columns of its tables itself.

use std::collections::BTreeSet;

use crate::read_set::ColumnSet;
use crate::select_syntax::{parts, window_parts, Part};
use rusqlite::Connection;
use sqlite3_parser::{
    ast::{
        Cmd, Expr, FromClause, JoinConstraint, JoinOperator, JoinType, OneSelect, QualifiedName,
        ResultColumn, Select, SelectTable, Stmt,
    },
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

/// The columns USING and NATURAL joins compare in `select`, as (table,
/// column) pairs, lowercase.
pub(crate) fn joined_columns(
    connection: &Connection,
    select: &Select<'_>,
) -> rusqlite::Result<ColumnSet> {
    let mut walk = Walk { connection, views: BTreeSet::new(), found: ColumnSet::new() };
    walk.select(select)?;
    Ok(walk.found)
}

struct Walk<'c> {
    connection: &'c Connection,
    /// The views read so far, lowercase, so a view read twice is read once.
    views: BTreeSet<String>,
    found: ColumnSet,
}

impl Walk<'_> {
    fn select(&mut self, select: &Select<'_>) -> rusqlite::Result<()> {
        for cte in select.with.iter().flat_map(|with| with.ctes.iter()) {
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
    /// column of each table before the join or joined that has it, and
    /// every column of each such table for a NATURAL join, which compares
    /// the columns the two sides share.
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
            let natural = matches!(
                join.operator,
                JoinOperator::TypedJoin(Some(kind)) if kind.contains(JoinType::NATURAL)
            );
            let using: Option<Vec<String>> = match &join.constraint {
                Some(JoinConstraint::Using(names)) => Some(
                    names
                        .iter()
                        .map(|name| crate::sql::name(name.0).to_ascii_lowercase())
                        .collect(),
                ),
                _ => None,
            };
            if !natural && using.is_none() {
                continue;
            }
            // The items up to the join's own, which sits after the first.
            for item in &items[..position + 2] {
                let SelectTable::Table(name, ..) = item else { continue };
                let Some(table) = self.object(name, "table")? else { continue };
                for column in self.columns(&table)? {
                    if natural || using.as_ref().is_some_and(|using| using.contains(&column)) {
                        self.found.insert((table.to_ascii_lowercase(), column));
                    }
                }
            }
        }
        Ok(())
    }

    fn item(&mut self, item: &SelectTable<'_>) -> rusqlite::Result<()> {
        match item {
            SelectTable::Table(name, ..) => {
                let Some(view) = self.object(name, "view")? else { return Ok(()) };
                if !self.views.insert(view.to_ascii_lowercase()) {
                    return Ok(());
                }
                let sql: String = self.connection.query_row(
                    "SELECT sql FROM main.sqlite_schema WHERE type='view' AND name=?1",
                    [&view],
                    |row| row.get(0),
                )?;
                let bump = Bump::new();
                let mut parser = Parser::new(&bump, sql.as_bytes());
                if let Ok(Some(Cmd::Stmt(Stmt::CreateView { select, .. }))) = parser.next() {
                    self.select(select)?;
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

    /// The stored name of the main schema's object of type `kind` that
    /// `name` names. A name of another schema reads no table of the app's.
    fn object(&self, name: &QualifiedName<'_>, kind: &str) -> rusqlite::Result<Option<String>> {
        if name.db_name.as_ref().is_some_and(|schema| *schema != "main") {
            return Ok(None);
        }
        let mut objects = self
            .connection
            .prepare_cached("SELECT name FROM pragma_table_list WHERE schema='main' AND type=?1")?;
        let found = objects
            .query_map([kind], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .find(|object| name.name == object.as_str());
        Ok(found)
    }

    /// A table's columns, lowercase.
    fn columns(&self, table: &str) -> rusqlite::Result<Vec<String>> {
        let mut columns =
            self.connection.prepare_cached("SELECT name FROM pragma_table_xinfo(?1,'main')")?;
        let names = columns
            .query_map([table], |row| row.get::<_, String>(0))?
            .map(|name| name.map(|name| name.to_ascii_lowercase()))
            .collect();
        names
    }
}
