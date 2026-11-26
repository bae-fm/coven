//! Whether a SELECT can give NULL in each of its result columns, followed
//! through every query a column passes through: subqueries in FROM, CTEs and
//! views. SQLite names a table's column as the origin of a result column that
//! passes it on, whatever those queries do to its rows, so the table's own
//! `NOT NULL` doesn't answer this.
//!
//! A column holds no NULL when each query it passes through takes it from a
//! column that holds none, and gives no NULL of its own: no join pads the
//! column's FROM item with NULLs, no aggregate without a GROUP BY makes a
//! row over no rows, and each SELECT of a compound holds none. A scalar
//! subquery returns NULL when it returns no row, and the analysis takes it,
//! as it takes every other computed expression, to be one that can give
//! NULL.

use rusqlite::Connection;
use sqlite3_parser::{
    ast::{
        As, Cmd, Expr, FromClause, JoinConstraint, JoinOperator, JoinType, Literal, Name,
        OneSelect, QualifiedName, ResultColumn, Select, SelectTable, Stmt,
    },
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

use crate::derived::{Ctes, Declared};
use crate::from_star::{star, StarItem};
use crate::schema_objects::{declared_columns, main_object, read_view, ROWID_NAMES};
use crate::schema_probe::table_column_never_null;
use crate::select_syntax::{aggregate_call, parts, parts_outside, Functions, Part};
use crate::sql::unparenthesized;

/// A column that a query or a FROM item gives.
#[derive(Clone)]
struct Column {
    /// Its name, lowercase, when it has one that SQL around it can use.
    name: Option<String>,
    /// Whether it can be NULL.
    nullable: bool,
}

/// A FROM item of a core.
struct Item {
    /// The name that qualifies its columns, lowercase.
    qualifier: Option<String>,
    /// Its columns, or `None` when the analysis doesn't know them.
    columns: Option<Vec<Column>>,
    /// Whether it is a table, which one of the rowid's names that no column
    /// of it has reads the rowid of.
    table: bool,
    /// Whether a join gives it NULL for the rows it doesn't join.
    padded: bool,
    /// The columns, lowercase, that USING or NATURAL compares with an item
    /// to its left, which a bare name and a `*` take from that item.
    using: Vec<String>,
}

impl Item {
    fn column(&self, name: &str) -> Option<&Column> {
        self.listed()?.iter().find(|column| column.name.as_deref() == Some(name))
    }

    /// `column`, one of its columns, as the core that reads the item gets it.
    fn read(&self, column: &Column) -> Column {
        Column { name: column.name.clone(), nullable: column.nullable || self.padded }
    }
}

impl StarItem for Item {
    type Column = Column;

    fn listed(&self) -> Option<&[Column]> {
        self.columns.as_deref()
    }

    fn using(&self) -> &[String] {
        &self.using
    }

    fn name(column: &Column) -> Option<&str> {
        column.name.as_deref()
    }
}

/// What a statement does to the NULLs of its result columns.
pub(crate) enum StatementNulls {
    /// A SELECT, with whether it can give NULL in each result column, in
    /// order.
    Select(Vec<bool>),
    /// A statement that writes: each column its RETURNING takes from the table
    /// it changes holds what the table's column holds.
    Write,
    /// A statement the analysis can't read, which can give NULL anywhere.
    Unknown,
}

/// What the statement `sql`, which SQLite prepared on `connection`, does to
/// the NULLs of its result columns.
pub(crate) fn statement_nulls(
    connection: &Connection,
    sql: &str,
) -> rusqlite::Result<StatementNulls> {
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    // SQLite and the parser can accept different language versions.
    let select = match parser.next() {
        Ok(Some(Cmd::Stmt(Stmt::Select(select)))) => select,
        Ok(Some(Cmd::Stmt(Stmt::Insert { .. } | Stmt::Update { .. } | Stmt::Delete { .. }))) => {
            return Ok(StatementNulls::Write)
        }
        Ok(_) => return Ok(StatementNulls::Unknown),
        Err(error) => {
            tracing::debug!(sql, %error, "the parser can't read a statement's columns");
            return Ok(StatementNulls::Unknown);
        }
    };
    let functions = Functions::read(connection)?;
    let analysis = Analysis { connection, functions: &functions };
    let columns = analysis.select(select, &Ctes::default())?;
    Ok(columns.map_or(StatementNulls::Unknown, |columns| {
        StatementNulls::Select(columns.into_iter().map(|column| column.nullable).collect())
    }))
}

struct Analysis<'c> {
    connection: &'c Connection,
    functions: &'c Functions,
}

impl Analysis<'_> {
    /// The columns of `select`, where its SQL can name `ctes`: those of its
    /// first SELECT, each of which can be NULL where any SELECT of the
    /// compound can give NULL in it.
    fn select<'a>(
        &self,
        select: &'a Select<'a>,
        ctes: &Ctes<'a>,
    ) -> rusqlite::Result<Option<Vec<Column>>> {
        let with;
        let ctes = match &select.with {
            Some(declared) => {
                // Nothing here reads a CTE's number or whether its WITH
                // stands alone.
                with = ctes.with(declared, false, &mut 0);
                &with
            }
            None => ctes,
        };
        let Some(mut columns) = self.core(&select.body.select, ctes)? else {
            return Ok(None);
        };
        for compound in select.body.compounds.iter().flatten() {
            let Some(arm) = self.core(&compound.select, ctes)? else {
                return Ok(None);
            };
            for (column, other) in columns.iter_mut().zip(arm) {
                column.nullable |= other.nullable;
            }
        }
        Ok(Some(columns))
    }

    /// The columns of one SELECT or VALUES.
    fn core<'a>(
        &self,
        core: &'a OneSelect<'a>,
        ctes: &Ctes<'a>,
    ) -> rusqlite::Result<Option<Vec<Column>>> {
        let (results, from, group_by) = match core {
            OneSelect::Values(rows) => {
                let width = rows.first().expect("a parsed VALUES has a row").len();
                let columns = crate::sql::values_names(width)
                    .into_iter()
                    .enumerate()
                    .map(|(position, name)| Column {
                        name: Some(name),
                        nullable: rows.iter().any(|row| !never_null_literal(&row[position])),
                    })
                    .collect();
                return Ok(Some(columns));
            }
            OneSelect::Select { columns, from, group_by, .. } => (columns, from, group_by),
        };
        let mut items = Vec::new();
        if let Some(from) = from {
            self.from(from, ctes, &mut items)?;
        }
        // An aggregate with no GROUP BY gives one row even over no rows,
        // which holds NULL in each column the core takes from a FROM item.
        // SQLite refuses a HAVING or an ORDER BY aggregate in a core whose
        // result columns call none.
        if group_by.is_none() && self.aggregates(results, &items) {
            for item in &mut items {
                item.padded = true;
            }
        }
        let mut columns = Vec::new();
        for result in results.iter() {
            match result {
                ResultColumn::Expr(expr, alias) => {
                    let mut column = expression(&items, expr);
                    if let Some(alias) = alias {
                        column.name = Some(crate::sql::alias_name(alias));
                    }
                    columns.push(column);
                }
                ResultColumn::Star => {
                    let Some(every) = star_columns(&items) else { return Ok(None) };
                    columns.extend(every);
                }
                ResultColumn::TableStar(qualifier) => {
                    let named = items.iter().find(|item| {
                        item.qualifier.as_deref().is_some_and(|name| *qualifier == Name(name))
                    });
                    let Some((item, listed)) = named.and_then(|item| Some((item, item.listed()?)))
                    else {
                        return Ok(None);
                    };
                    columns.extend(listed.iter().map(|column| item.read(column)));
                }
            }
        }
        Ok(Some(columns))
    }

    /// Whether the result columns `results` of a core whose FROM items are
    /// `items` call an aggregate of the core's rows, in their own
    /// expressions or in a subquery of theirs.
    fn aggregates<'a>(&self, results: &'a [ResultColumn<'a>], items: &[Item]) -> bool {
        let mut found = Vec::new();
        for expr in result_expressions(results) {
            parts(expr, &mut found);
        }
        found.iter().any(|part| match part {
            Part::Subquery(select) | Part::Select(select) => {
                self.aggregates_around(select, items, &mut Vec::new())
            }
            part => part.aggregates(self.functions),
        })
    }

    /// Whether the subquery `select` calls an aggregate that SQLite gives to
    /// a core around it whose FROM items are `items`: one that qualifies its
    /// columns, each with a name of `items` that no FROM between them has.
    /// `between` holds the names of those FROMs. SQL that passes an
    /// aggregate an outer column writes the column's qualifier (sqlive's
    /// SPEC.md, section 12).
    fn aggregates_around<'a>(
        &self,
        select: &'a Select<'a>,
        items: &[Item],
        between: &mut Vec<&'a str>,
    ) -> bool {
        let cores = std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().flatten().map(|compound| &compound.select));
        for core in cores {
            let OneSelect::Select { columns, from, .. } = core else { continue };
            let outside = between.len();
            if let Some(from) = from {
                from_names(from, between);
            }
            let mut calls = Vec::new();
            let mut found = Vec::new();
            for expr in result_expressions(columns) {
                let aggregate = |expr| aggregate_call(expr, self.functions).is_some();
                parts_outside(expr, &aggregate, &mut found, &mut calls);
            }
            for call in calls {
                let first = found.len();
                parts(call, &mut found);
                let mut qualifiers = found[first..]
                    .iter()
                    .filter_map(|part| match part {
                        Part::Name(name) => match unparenthesized(name) {
                            Expr::Qualified(table, _) | Expr::DoublyQualified(_, table, _) => {
                                Some(table)
                            }
                            _ => None,
                        },
                        _ => None,
                    })
                    .peekable();
                let named_around = qualifiers.peek().is_some()
                    && qualifiers.all(|qualifier| {
                        !between.iter().any(|name| *qualifier == Name(name))
                            && items.iter().any(|item| {
                                item.qualifier
                                    .as_deref()
                                    .is_some_and(|name| *qualifier == Name(name))
                            })
                    });
                if named_around {
                    return true;
                }
            }
            for part in found {
                if let Part::Subquery(inner) | Part::Select(inner) = part {
                    if self.aggregates_around(inner, items, between) {
                        return true;
                    }
                }
            }
            between.truncate(outside);
        }
        false
    }

    /// Adds the items of `from`, a core's FROM or the tables of a join in
    /// parentheses, to `items`, each padded as its joins pad it.
    fn from<'a>(
        &self,
        from: &'a FromClause<'a>,
        ctes: &Ctes<'a>,
        items: &mut Vec<Item>,
    ) -> rusqlite::Result<()> {
        let first = items.len();
        let tables = from
            .select
            .iter()
            .map(|table| (*table, None))
            .chain(from.joins.iter().flatten().map(|join| (&join.table, Some(join))));
        for (table, join) in tables {
            let before = items.len();
            self.table(table, ctes, items)?;
            let Some(join) = join else { continue };
            let kind = match join.operator {
                JoinOperator::TypedJoin(Some(kind)) => kind,
                JoinOperator::Comma | JoinOperator::TypedJoin(None) => JoinType::empty(),
            };
            let (left, right) = items.split_at_mut(before);
            if let Some(JoinConstraint::Using(names)) = &join.constraint {
                for item in right.iter_mut() {
                    item.using.extend(
                        names.iter().map(|name| crate::sql::name(name.0).to_ascii_lowercase()),
                    );
                }
            }
            if kind.contains(JoinType::NATURAL) {
                for item in right.iter_mut() {
                    let shared = item
                        .columns
                        .iter()
                        .flatten()
                        .filter_map(|column| column.name.clone())
                        .filter(|name| {
                            left[first..].iter().any(|item| item.column(name).is_some())
                        });
                    item.using = shared.collect();
                }
            }
            if kind.contains(JoinType::LEFT) {
                for item in right {
                    item.padded = true;
                }
            }
            if kind.contains(JoinType::RIGHT) {
                for item in &mut left[first..] {
                    item.padded = true;
                }
            }
        }
        Ok(())
    }

    /// Adds the item or items that `table` is to `items`.
    fn table<'a>(
        &self,
        table: &'a SelectTable<'a>,
        ctes: &Ctes<'a>,
        items: &mut Vec<Item>,
    ) -> rusqlite::Result<()> {
        let alias_or = |alias: &Option<As<'_>>, name: Option<&Name<'_>>| {
            alias
                .as_ref()
                .map(crate::sql::alias_name)
                .or_else(|| name.map(|name| crate::sql::name(name.0).to_ascii_lowercase()))
        };
        let derived = |qualifier, columns| Item {
            qualifier,
            columns,
            table: false,
            padded: false,
            using: Vec::new(),
        };
        match table {
            SelectTable::Table(name, alias, _) => {
                let qualifier = alias_or(alias, Some(&name.name));
                let cte = name.db_name.is_none().then(|| ctes.named(&name.name)).flatten();
                let item = match cte {
                    Some(declared) => derived(qualifier, self.cte(declared, ctes)?),
                    None => self.schema_item(name, qualifier)?,
                };
                items.push(item);
            }
            SelectTable::TableCall(name, _, alias) => {
                // A table function's arguments choose its rows, and any of
                // its columns can be NULL.
                let columns = self.any_nulls(&crate::sql::name(name.name.0))?;
                items.push(derived(alias_or(alias, Some(&name.name)), columns));
            }
            SelectTable::Select(select, alias) => {
                let columns = self.select(select, ctes)?;
                items.push(derived(alias_or(alias, None), columns));
            }
            SelectTable::Sub(inner, alias) => {
                let first = items.len();
                self.from(inner, ctes, items)?;
                // Under an alias, the join's rows are a subquery's.
                if let Some(alias) = alias_or(alias, None) {
                    let columns = star_columns(&items[first..]);
                    items.truncate(first);
                    items.push(derived(Some(alias), columns));
                }
            }
        }
        Ok(())
    }

    /// The columns a `*` gives of the view or table function `stored`, each
    /// of which can be NULL, or `None` when SQLite lists none.
    fn any_nulls(&self, stored: &str) -> rusqlite::Result<Option<Vec<Column>>> {
        let columns: Vec<Column> = declared_columns(self.connection, stored)?
            .into_iter()
            .filter(|column| !column.hidden)
            .map(|column| Column { name: Some(column.name.to_ascii_lowercase()), nullable: true })
            .collect();
        Ok((!columns.is_empty()).then_some(columns))
    }

    /// The columns of the CTE `declared`, under the names its column list
    /// gives them, when it has one. A recursive CTE's columns are unknown
    /// where it reads itself.
    fn cte<'a>(
        &self,
        declared: Declared<'a>,
        ctes: &Ctes<'a>,
    ) -> rusqlite::Result<Option<Vec<Column>>> {
        let cte = declared.cte;
        let names = cte.columns.map(crate::sql::column_list_names);
        let Some(inner) = ctes.reading(&declared) else {
            return Ok(names.map(|names| {
                names.into_iter().map(|name| Column { name: Some(name), nullable: true }).collect()
            }));
        };
        Ok(self.select(cte.select, &inner)?.map(|columns| renamed(columns, names)))
    }

    /// The item that reads the schema's table or view `name`.
    fn schema_item(
        &self,
        name: &QualifiedName<'_>,
        qualifier: Option<String>,
    ) -> rusqlite::Result<Item> {
        let mut item =
            Item { qualifier, columns: None, table: false, padded: false, using: Vec::new() };
        if name.db_name.as_ref().is_some_and(|database| *database != "main") {
            return Ok(item);
        }
        let Some((stored, kind)) = main_object(self.connection, &name.name)? else {
            // Another virtual table.
            item.columns = self.any_nulls(&crate::sql::name(name.name.0))?;
            return Ok(item);
        };
        if kind == "view" {
            let read = read_view(self.connection, &stored, |names, select| {
                let names = names.map(crate::sql::column_list_names);
                Ok(self.select(select, &Ctes::default())?.map(|columns| renamed(columns, names)))
            })?;
            let columns = match read {
                Ok(columns) => columns,
                Err(why) => {
                    tracing::debug!(view = stored, %why, "the parser can't read a view's columns");
                    None
                }
            };
            // A view the analysis can't read can give NULL in any column.
            item.columns = match columns {
                Some(columns) => Some(columns),
                None => self.any_nulls(&stored)?,
            };
            return Ok(item);
        }
        let ordinary = kind == "table";
        let mut columns = Vec::new();
        for column in declared_columns(self.connection, &stored)? {
            // A table's hidden columns are its generated ones, which a `*`
            // gives; a virtual table's it leaves out.
            if column.hidden && !ordinary {
                continue;
            }
            let never_null = table_column_never_null(self.connection, &stored, &column.name)?;
            columns.push(Column {
                name: Some(column.name.to_ascii_lowercase()),
                nullable: !never_null,
            });
        }
        if !columns.is_empty() {
            item.columns = Some(columns);
            item.table = true;
        }
        Ok(item)
    }
}

/// The expressions of the result columns `results`.
fn result_expressions<'a>(results: &'a [ResultColumn<'a>]) -> impl Iterator<Item = &'a Expr<'a>> {
    results.iter().filter_map(|result| match result {
        ResultColumn::Expr(expr, _) => Some(expr),
        ResultColumn::Star | ResultColumn::TableStar(_) => None,
    })
}

/// Adds the names that qualify the columns of `from`'s items to `names`, as
/// the SQL writes them.
fn from_names<'a>(from: &'a FromClause<'a>, names: &mut Vec<&'a str>) {
    let tables =
        from.select.iter().copied().chain(from.joins.iter().flatten().map(|join| &join.table));
    for table in tables {
        let (alias, name) = match table {
            SelectTable::Table(name, alias, _) | SelectTable::TableCall(name, _, alias) => {
                (alias, Some(&name.name))
            }
            SelectTable::Select(_, alias) => (alias, None),
            SelectTable::Sub(inner, alias) => {
                from_names(inner, names);
                (alias, None)
            }
        };
        match alias {
            Some(As::As(alias) | As::Elided(alias)) => names.push(alias.0),
            None => names.extend(name.map(|name| name.0)),
        }
    }
}

/// `columns` under `names`, a column list that SQLite holds to their length.
fn renamed(mut columns: Vec<Column>, names: Option<Vec<String>>) -> Vec<Column> {
    for (column, name) in columns.iter_mut().zip(names.into_iter().flatten()) {
        column.name = Some(name);
    }
    columns
}

/// The column that the result expression `expr` of a core whose FROM items are
/// `items` gives.
fn expression(items: &[Item], expr: &Expr<'_>) -> Column {
    let computed = Column { name: None, nullable: !never_null_literal(expr) };
    let (qualifier, name) = match unparenthesized(expr) {
        Expr::Id(name) => (None, name.0),
        Expr::Name(name) => (None, name.0),
        Expr::Qualified(table, name) | Expr::DoublyQualified(_, table, name) => {
            (Some(table), name.0)
        }
        _ => return computed,
    };
    let name = crate::sql::name(name).to_ascii_lowercase();
    let named = items.iter().filter(|item| match qualifier {
        Some(qualifier) => item.qualifier.as_deref().is_some_and(|item| *qualifier == Name(item)),
        None => true,
    });
    // SQLite takes a bare name that USING compares from the leftmost item.
    let mut rowids = Vec::new();
    for item in named {
        if let Some(column) = item.column(&name) {
            return item.read(column);
        }
        // An item of unknown columns may be the one that has the name.
        if item.columns.is_none() {
            return Column { name: Some(name), nullable: true };
        }
        if item.table {
            rowids.push(item.padded);
        }
    }
    match rowids[..] {
        [padded] if ROWID_NAMES.contains(&name.as_str()) => {
            Column { name: Some(name), nullable: padded }
        }
        // A name of a row around the core, or none of any item's.
        _ => Column { name: Some(name), nullable: true },
    }
}

/// Whether `expr` is a literal other than NULL.
fn never_null_literal(expr: &Expr<'_>) -> bool {
    matches!(
        unparenthesized(expr),
        Expr::Literal(Literal::Numeric(_) | Literal::String(_) | Literal::Blob(_))
    )
}

/// The columns a `*` gives of `items`, as the core that reads them gets
/// them.
fn star_columns(items: &[Item]) -> Option<Vec<Column>> {
    Some(star(items)?.into_iter().map(|(at, column)| items[at].read(column)).collect())
}

#[cfg(test)]
#[path = "result_nulls_tests.rs"]
mod tests;
