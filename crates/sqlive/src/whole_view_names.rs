//! The names SQLite can report a read through while it runs a view that
//! sqlive reads whole (§4). SQLite's authorizer names the innermost view or
//! CTE each read is made through, so a read inside such a view comes with
//! the view's own name, the name of a CTE its SQL declares, or a name from
//! a view it reads. A statement's own CTE can share such a name, so a read
//! reported through it may belong to the view rather than to the CTE.

use std::collections::BTreeSet;

use crate::schema_objects::{main_object, read_view};
use crate::select_syntax::{parts, window_parts, Part};
use rusqlite::Connection;
use sqlite3_parser::ast::{
    Expr, FromClause, JoinConstraint, OneSelect, QualifiedName, ResultColumn, Select, SelectTable,
};

/// The names, lowercase, SQLite can report a read through while it runs
/// the main schema's view `stored`: its own, each CTE its SQL declares, and
/// those of each view its SQL reads; or the parser's message when it can't
/// read one of those views, whose SQL could declare any name.
pub(crate) fn whole_view_names(
    connection: &Connection,
    stored: &str,
) -> rusqlite::Result<Result<BTreeSet<String>, String>> {
    let mut walk =
        Walk { connection, names: BTreeSet::new(), walked: BTreeSet::new(), unread: None };
    walk.view(stored)?;
    Ok(match walk.unread {
        Some(why) => Err(why),
        None => Ok(walk.names),
    })
}

struct Walk<'c> {
    connection: &'c Connection,
    names: BTreeSet<String>,
    /// The views read so far, lowercase, whose names are among `names`. A
    /// CTE can share a view's name, so `names` alone doesn't say which.
    walked: BTreeSet<String>,
    /// Why the parser couldn't read a view the walk met.
    unread: Option<String>,
}

impl Walk<'_> {
    fn view(&mut self, stored: &str) -> rusqlite::Result<()> {
        let name = stored.to_ascii_lowercase();
        // A view read already added its names.
        if !self.walked.insert(name.clone()) {
            return Ok(());
        }
        self.names.insert(name);
        let connection = self.connection;
        if let Err(why) = read_view(connection, stored, |_, select| self.select(select))? {
            self.unread.get_or_insert(why);
        }
        Ok(())
    }

    fn select(&mut self, select: &Select<'_>) -> rusqlite::Result<()> {
        for cte in select.with.iter().flat_map(|with| with.ctes.iter()) {
            self.names.insert(crate::sql::name(cte.tbl_name.0).to_ascii_lowercase());
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
        let mut found = Vec::new();
        match core {
            OneSelect::Values(rows) => {
                for expr in rows.iter().flat_map(|row| row.iter()) {
                    parts(expr, &mut found);
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
                        parts(expr, &mut found);
                    }
                }
                if let Some(from) = from {
                    self.from(from)?;
                }
                for expr in where_clause.iter().copied().chain(having.iter().copied()) {
                    parts(expr, &mut found);
                }
                for expr in group_by.iter().flat_map(|exprs| exprs.iter()) {
                    parts(expr, &mut found);
                }
                for definition in window_clause.iter().flat_map(|windows| windows.iter()) {
                    window_parts(&definition.window, &mut found);
                }
            }
        }
        self.parts(&found)
    }

    fn from(&mut self, from: &FromClause<'_>) -> rusqlite::Result<()> {
        for table in
            from.select.iter().copied().chain(from.joins.iter().flatten().map(|join| &join.table))
        {
            self.item(table)?;
        }
        for join in from.joins.iter().flatten() {
            if let Some(JoinConstraint::On(expr)) = &join.constraint {
                self.expr(expr)?;
            }
        }
        Ok(())
    }

    fn item(&mut self, item: &SelectTable<'_>) -> rusqlite::Result<()> {
        match item {
            SelectTable::Table(name, ..) => self.named(name),
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

    /// A table, view or CTE that `name` names: a view's names count. A CTE's
    /// name is one the WITH declaring it added already; a view that shares
    /// it counts too, whichever of them SQLite reads.
    fn named(&mut self, name: &QualifiedName<'_>) -> rusqlite::Result<()> {
        if name.db_name.as_ref().is_some_and(|db| *db != "main") {
            return Ok(());
        }
        match main_object(self.connection, &name.name)? {
            Some((view, kind)) if kind == "view" => self.view(&view),
            _ => Ok(()),
        }
    }

    fn expr(&mut self, expr: &Expr<'_>) -> rusqlite::Result<()> {
        let mut found = Vec::new();
        parts(expr, &mut found);
        self.parts(&found)
    }

    fn parts(&mut self, found: &[Part<'_>]) -> rusqlite::Result<()> {
        for part in found {
            match part {
                Part::Select(select) | Part::Subquery(select) => self.select(select)?,
                Part::Table(name) => self.named(name)?,
                Part::Name(_) | Part::Collation | Part::Function(..) | Part::Clock => {}
            }
        }
        Ok(())
    }
}
