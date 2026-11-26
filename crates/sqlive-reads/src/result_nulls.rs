//! Whether a SELECT can give NULL in each of its result columns, followed
//! through every query a column passes through: subqueries in FROM, CTEs and
//! views. SQLite names a table's column as the origin of a result column that
//! passes it on, whatever those queries do to its rows, so the table's own
//! `NOT NULL` doesn't answer this.
//!
//! A column holds no NULL when each query it passes through takes it from a
//! column that holds none, or keeps only the rows where it holds none: those
//! of a SELECT whose WHERE requires it `IS NOT NULL`. Each query also gives
//! no NULL of its own: no join pads the column's FROM item with NULLs in a row
//! it keeps, no aggregate without a GROUP BY makes a row over no rows, and
//! each SELECT of a compound holds none. A scalar
//! subquery returns NULL when it returns no row, so it holds no NULL only
//! where it returns one whose value never is (`never_null`). Of the other
//! computed expressions, the analysis takes each but a literal to be one that
//! can give NULL.
//!
//! It also says of each column whether the statement can give no row of the
//! FROM item the column comes from: a join pads that item, or an aggregate
//! with no GROUP BY makes a row over no rows. And it says what each SELECT of
//! the statement's compound can give the column.

use rusqlite::Connection;
use sqlite3_parser::{
    ast::{
        As, Cmd, Expr, FromClause, JoinConstraint, JoinOperator, JoinType, Literal, Name,
        OneSelect, Operator, QualifiedName, ResultColumn, Select, SelectTable, Stmt,
    },
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

use crate::derived::{Ctes, Declared};
use crate::from_star::{star, StarItem};
use crate::never_null;
use crate::schema_objects::{declared_columns, main_object, read_view, ROWID_NAMES};
use crate::schema_probe::table_column_never_null;
use crate::select_syntax::{
    aggregate_owners, conjuncts, parts, result_expressions, Functions, Part,
};
use crate::sql::unparenthesized;

/// Where a statement can give NULL in one of its result columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnNulls {
    /// What each SELECT of the statement's compound can give the column, in
    /// order, or what its one SELECT or write can.
    pub selects: Vec<SelectNull>,
    /// Whether the statement can give no row of the FROM item it takes the
    /// column from, and so NULL in each of that item's columns: a join pads
    /// the item, or an aggregate with no GROUP BY makes a row over no rows.
    /// It speaks of the first SELECT's own FROM items: a column of a subquery
    /// there says nothing of the rows missing inside the subquery. A computed
    /// column comes from no item, and a RETURNING's column from the row the
    /// write changed, so neither has a missing row.
    pub row_can_be_missing: bool,
}

impl ColumnNulls {
    /// Whether a row the statement returns can hold NULL in the column, as far
    /// as the probe can tell.
    pub fn nullable(&self) -> bool {
        self.selects.iter().any(|select| *select != SelectNull::Never)
    }
}

/// What one SELECT can give a result column. It takes a column through every
/// query the column passes through. A table's column holds no NULL when the
/// table declares it `NOT NULL`, or it is the rowid, by one of the rowid's own
/// names or as an `INTEGER PRIMARY KEY` column. No column holds one in a
/// SELECT whose WHERE requires it `IS NOT NULL`. In a write, only a table's
/// column and a scalar subquery can be ones that hold none: each other
/// computed column of a RETURNING, a literal too, is one that can be NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectNull {
    /// No NULL: a table's column that holds none, a column that the WHERE
    /// requires `IS NOT NULL`, a literal other than NULL, or a scalar subquery
    /// that never gives NULL.
    Never,
    /// A computed value other than a literal and a scalar subquery, which can
    /// be NULL as far as the probe can tell: the probe reads no computation.
    Computed,
    /// Always NULL: the literal NULL.
    Literal,
    /// NULL where the SELECT has no row of one of its FROM items, and in each
    /// row it has of that item a value, or a computed one. Two columns of one
    /// SELECT with the same number come from the same item.
    MissingRow(usize),
    /// A value that can be NULL otherwise: a table's column that can be,
    /// another scalar subquery, or a column the probe can't follow.
    Value,
}

impl SelectNull {
    /// What the column of a query can give that each of whose SELECTs, or
    /// rows of a VALUES, gives it one of `selects`, to the SQL that reads the
    /// query.
    fn through(selects: &[Self]) -> Self {
        let every = |one_of: &[Self]| selects.iter().all(|select| one_of.contains(select));
        if every(&[Self::Never]) {
            Self::Never
        } else if every(&[Self::Literal]) {
            Self::Literal
        } else if every(&[Self::Never, Self::Computed]) {
            Self::Computed
        } else {
            // A missing row inside the query is none of its reader's.
            Self::Value
        }
    }

    /// What the computed expression `expr` can give.
    fn of_computed(expr: &Expr<'_>, functions: &Functions) -> Self {
        match unparenthesized(expr) {
            Expr::Literal(Literal::Numeric(_) | Literal::String(_) | Literal::Blob(_)) => {
                Self::Never
            }
            Expr::Literal(Literal::Null) => Self::Literal,
            Expr::Subquery(select) if never_null::subquery(select, functions) => Self::Never,
            Expr::Subquery(_) => Self::Value,
            _ => Self::Computed,
        }
    }
}

/// A column that a query or a FROM item gives.
#[derive(Clone)]
struct Column {
    /// Its name, lowercase, when it has one that SQL around it can use.
    name: Option<String>,
    /// What its NULLs are. Where a core reads the column, `Item::read` says
    /// them of that core.
    nulls: ColumnNulls,
}

impl Column {
    /// A column called `name` of one SELECT, which gives it `null`.
    fn new(name: Option<String>, null: SelectNull) -> Self {
        Self { name, nulls: ColumnNulls { selects: vec![null], row_can_be_missing: false } }
    }
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
    /// Whether the core can give no row of it, and NULL in each of its
    /// columns: a join gives it NULL for the rows it doesn't join, or the
    /// core's aggregate makes a row over no rows.
    padded: bool,
    /// Its place among its core's items, which tells it from them.
    number: usize,
    /// The columns, lowercase, that USING or NATURAL compares with an item
    /// to its left, which a bare name and a `*` take from that item.
    using: Vec<String>,
    /// Whether a RIGHT or FULL JOIN joins it, after which SQLite can read the
    /// bare name of a column in `using` from this item.
    right_joined: bool,
}

impl Item {
    fn column(&self, name: &str) -> Option<&Column> {
        self.listed()?.iter().find(|column| column.name.as_deref() == Some(name))
    }

    /// `column`, one of its columns, as the core that reads the item gets it.
    fn read(&self, column: &Column) -> Column {
        let null = match SelectNull::through(&column.nulls.selects) {
            SelectNull::Never | SelectNull::Computed if self.padded => {
                SelectNull::MissingRow(self.number)
            }
            null => null,
        };
        Column {
            name: column.name.clone(),
            nulls: ColumnNulls { selects: vec![null], row_can_be_missing: self.padded },
        }
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

/// What a column name in a core reads.
enum Reference {
    /// The column at `column` among those that the item at `item` lists.
    Listed { item: usize, column: usize },
    /// A column of an item whose columns the analysis doesn't know, which may
    /// be the one that has the name.
    Unlisted(usize),
    /// A table item's rowid, by one of the rowid's own names.
    Rowid(usize),
    /// No item's column: a name of a row around the core, or none at all.
    Outside,
}

/// What the column name `name`, lowercase, reads in a core whose FROM items
/// are `items`, where `qualifier` qualifies it.
fn reference(items: &[Item], qualifier: Option<&Name<'_>>, name: &str) -> Reference {
    let named = items.iter().enumerate().filter(|(_, item)| match qualifier {
        Some(qualifier) => item.qualifier.as_deref().is_some_and(|item| *qualifier == Name(item)),
        None => true,
    });
    // SQLite takes a bare name that USING compares from the leftmost item.
    let mut rowids = Vec::new();
    for (at, item) in named {
        let Some(listed) = item.listed() else { return Reference::Unlisted(at) };
        if let Some(column) = listed.iter().position(|column| column.name.as_deref() == Some(name))
        {
            return Reference::Listed { item: at, column };
        }
        if item.table {
            rowids.push(at);
        }
    }
    match rowids[..] {
        [item] if ROWID_NAMES.contains(&name) => Reference::Rowid(item),
        _ => Reference::Outside,
    }
}

/// The column name that `expr` is, lowercase, with the table that qualifies
/// it, or `None` when it is no column name.
fn column_name<'a, 'b>(expr: &'a Expr<'b>) -> Option<(Option<&'a Name<'b>>, String)> {
    let (qualifier, name) = match unparenthesized(expr) {
        Expr::Id(name) => (None, name.0),
        Expr::Name(name) => (None, name.0),
        Expr::Qualified(table, name) | Expr::DoublyQualified(_, table, name) => {
            (Some(table), name.0)
        }
        _ => return None,
    };
    Some((qualifier, crate::sql::name(name).to_ascii_lowercase()))
}

/// What `term` holds to be other than NULL, when that is all it asks:
/// `x IS NOT NULL`, `x NOT NULL` or `x NOTNULL`.
fn required_not_null<'a, 'b>(term: &'a Expr<'b>) -> Option<&'a Expr<'b>> {
    let null = |expr: &Expr<'_>| matches!(unparenthesized(expr), Expr::Literal(Literal::Null));
    match term {
        Expr::NotNull(expr) => Some(expr),
        Expr::Binary(left, Operator::IsNot, right) if null(right) => Some(left),
        Expr::Binary(left, Operator::IsNot, right) if null(left) => Some(right),
        _ => None,
    }
}

/// Applies the WHERE `predicate` to `items`, its core's FROM items. A column
/// that one of the terms it joins with AND requires `IS NOT NULL` holds no
/// NULL in a row the core keeps, and no such row lacks a row of the column's
/// item, which would hold NULL there.
fn filter<'a>(items: &mut [Item], predicate: &'a Expr<'a>) {
    let mut terms = Vec::new();
    conjuncts(predicate, &mut terms);
    for (qualifier, name) in terms.into_iter().filter_map(required_not_null).filter_map(column_name)
    {
        // Such a name's value can be another item's than the one it would
        // read in a result column, or either's.
        if qualifier.is_none()
            && items.iter().any(|item| item.right_joined && item.using.contains(&name))
        {
            continue;
        }
        match reference(items, qualifier, &name) {
            Reference::Listed { item, column } => {
                items[item].padded = false;
                let columns = items[item].columns.as_mut().expect("the item lists the column");
                columns[column] = Column::new(Some(name), SelectNull::Never);
            }
            Reference::Rowid(item) => items[item].padded = false,
            Reference::Unlisted(_) | Reference::Outside => {}
        }
    }
}

/// What a statement does to the NULLs of its result columns.
pub(crate) enum StatementNulls {
    /// A SELECT, with what it does to each result column, in order.
    Select(Vec<ColumnNulls>),
    /// A statement that writes: each column its RETURNING takes from the table
    /// it changes holds what the table's column holds, and each scalar
    /// subquery of it holds what the subquery gives, which is here for each
    /// column that one gives, in order.
    Write(Vec<Option<SelectNull>>),
    /// A statement the analysis can't read, which can give NULL anywhere.
    Unknown,
}

/// For each of the `width` columns of the RETURNING whose results are
/// `results`, what the scalar subquery that gives it can give, or `None` when
/// no scalar subquery does.
fn returning_subqueries(
    results: &[ResultColumn<'_>],
    width: usize,
    functions: &Functions,
) -> Vec<Option<SelectNull>> {
    let stars = results.iter().filter(|result| matches!(result, ResultColumn::Star)).count();
    // A result that is no `*` gives one column. SQLite refuses a `table.*`
    // here, so each `*` gives every column of the table the statement changes.
    let star_width = (width + stars - results.len()).checked_div(stars).unwrap_or_default();
    let columns = results.iter().flat_map(|result| match result {
        ResultColumn::Star => vec![None; star_width],
        ResultColumn::TableStar(_) => unreachable!("SQLite refuses a `table.*` in a RETURNING"),
        ResultColumn::Expr(expr, _) => vec![match unparenthesized(expr) {
            Expr::Subquery(_) => Some(SelectNull::of_computed(expr, functions)),
            _ => None,
        }],
    });
    columns.collect()
}

/// What the statement `sql`, which SQLite prepared on `connection` with
/// `width` result columns, does to their NULLs.
pub(crate) fn statement_nulls(
    connection: &Connection,
    sql: &str,
    width: usize,
) -> rusqlite::Result<StatementNulls> {
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    // SQLite and the parser can accept different language versions.
    let statement = match parser.next() {
        Ok(Some(Cmd::Stmt(statement))) => statement,
        Ok(_) => return Ok(StatementNulls::Unknown),
        Err(error) => {
            tracing::debug!(sql, %error, "the parser can't read a statement's columns");
            return Ok(StatementNulls::Unknown);
        }
    };
    let functions = Functions::read(connection)?;
    let select = match statement {
        Stmt::Select(select) => select,
        Stmt::Insert { returning, .. }
        | Stmt::Update { returning, .. }
        | Stmt::Delete { returning, .. } => {
            let results = returning.unwrap_or_default();
            return Ok(StatementNulls::Write(returning_subqueries(results, width, &functions)));
        }
        _ => return Ok(StatementNulls::Unknown),
    };
    let analysis = Analysis { connection, functions: &functions };
    let columns = analysis.select(select, &Ctes::default())?;
    Ok(columns.map_or(StatementNulls::Unknown, |columns| {
        StatementNulls::Select(columns.into_iter().map(|column| column.nulls).collect())
    }))
}

struct Analysis<'c> {
    connection: &'c Connection,
    functions: &'c Functions,
}

impl Analysis<'_> {
    /// The columns of `select`, where its SQL can name `ctes`: those of its
    /// first SELECT, each with what every SELECT of the compound can give
    /// it.
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
                column.nulls.selects.extend(other.nulls.selects);
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
        let (results, from, where_clause, group_by) = match core {
            OneSelect::Values(rows) => {
                let width = rows.first().expect("a parsed VALUES has a row").len();
                let columns = crate::sql::values_names(width)
                    .into_iter()
                    .enumerate()
                    .map(|(position, name)| {
                        let rows: Vec<_> = rows
                            .iter()
                            .map(|row| SelectNull::of_computed(&row[position], self.functions))
                            .collect();
                        Column::new(Some(name), SelectNull::through(&rows))
                    })
                    .collect();
                return Ok(Some(columns));
            }
            OneSelect::Select { columns, from, where_clause, group_by, .. } => {
                (columns, from, where_clause, group_by)
            }
        };
        let mut items = Vec::new();
        if let Some(from) = from {
            self.from(from, ctes, &mut items)?;
        }
        for (number, item) in items.iter_mut().enumerate() {
            item.number = number;
        }
        if let Some(predicate) = where_clause {
            filter(&mut items, predicate);
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
                    let mut column = self.expression(&items, expr);
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
    /// a core around it whose FROM items are `items`: one that goes past each
    /// FROM between them (`aggregate_owners`) and qualifies its columns, each
    /// with a name of `items`. `between` holds the names of those FROMs.
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
            let mut found = Vec::new();
            let owners =
                aggregate_owners(columns, from.as_ref(), between, self.functions, &mut found);
            let named_around = |qualifiers: Vec<&Name<'_>>| {
                qualifiers.into_iter().all(|qualifier| {
                    items.iter().any(|item| {
                        item.qualifier.as_deref().is_some_and(|name| *qualifier == Name(name))
                    })
                })
            };
            if owners.into_iter().flatten().any(named_around) {
                return true;
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

    /// The column that the result expression `expr` of a core whose FROM items
    /// are `items` gives.
    fn expression(&self, items: &[Item], expr: &Expr<'_>) -> Column {
        let Some((qualifier, name)) = column_name(expr) else {
            return Column::new(None, SelectNull::of_computed(expr, self.functions));
        };
        match reference(items, qualifier, &name) {
            Reference::Listed { item, column } => {
                let item = &items[item];
                item.read(&item.listed().expect("the item lists the column")[column])
            }
            Reference::Unlisted(item) => {
                items[item].read(&Column::new(Some(name), SelectNull::Value))
            }
            Reference::Rowid(item) => items[item].read(&Column::new(Some(name), SelectNull::Never)),
            // SQLite reads `true` and `false` as numbers where no column has the
            // name.
            Reference::Outside if name == "true" || name == "false" => {
                Column::new(Some(name), SelectNull::Never)
            }
            Reference::Outside => Column::new(Some(name), SelectNull::Value),
        }
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
                for item in right.iter_mut() {
                    item.padded = true;
                }
            }
            if kind.contains(JoinType::RIGHT) {
                for item in right.iter_mut() {
                    item.right_joined = true;
                }
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
            number: 0,
            using: Vec::new(),
            right_joined: false,
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
            .map(|column| Column::new(Some(column.name.to_ascii_lowercase()), SelectNull::Value))
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
                names.into_iter().map(|name| Column::new(Some(name), SelectNull::Value)).collect()
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
        let mut item = Item {
            qualifier,
            columns: None,
            table: false,
            padded: false,
            number: 0,
            using: Vec::new(),
            right_joined: false,
        };
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
            let null = if never_null { SelectNull::Never } else { SelectNull::Value };
            columns.push(Column::new(Some(column.name.to_ascii_lowercase()), null));
        }
        if !columns.is_empty() {
            item.columns = Some(columns);
            item.table = true;
        }
        Ok(item)
    }
}

/// `columns` under `names`, a column list that SQLite holds to their length.
fn renamed(mut columns: Vec<Column>, names: Option<Vec<String>>) -> Vec<Column> {
    for (column, name) in columns.iter_mut().zip(names.into_iter().flatten()) {
        column.name = Some(name);
    }
    columns
}

/// The columns a `*` gives of `items`, as the core that reads them gets
/// them.
fn star_columns(items: &[Item]) -> Option<Vec<Column>> {
    Some(star(items)?.into_iter().map(|(at, column)| items[at].read(column)).collect())
}

#[cfg(test)]
#[path = "result_nulls_tests.rs"]
mod tests;
