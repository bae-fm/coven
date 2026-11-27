//! Whether the terms a sort compares, with the equalities of a SELECT's
//! conditions, tell every two of its rows apart (SPEC.md section 6). The rows
//! a SELECT joins are told apart when each of its FROM items adds one row at
//! most to each row of those before it. A table adds one at most when the sort
//! and the equalities with the items told apart before it cover a unique key
//! of the table, its rowid among them. A FROM subquery adds one at most when
//! the equalities with the columns it returns let it return one row at most.
//! The model states the rule as `Statement.tellsApart` and
//! `Statement.oneRow` in `crates/sqlive/proofs/SqliveNarrowing/Tells.lean`,
//! and `Rules/TellsApart.lean` proves it.

use std::collections::BTreeSet;

use crate::comparison::{
    at_least_as_fine, comparison_collation, keeps_values, Carried, Traits, BINARY,
};
use crate::from_item::{Column, ColumnValue, Columns};
use crate::schema_objects::{MainSchema, ROWID_NAMES};
use crate::scope_term::Affinity;
use crate::select_syntax::{conjuncts, order_term, parts, Functions, OrderTerm, Part};
use crate::sql::unparenthesized;
use crate::sql_walk::{Visit, Walk};
use crate::table_keys::{is_rowid, not_null_columns, unique_keys, ROWID};
use rusqlite::Connection;
use sqlite3_parser::ast::{
    As, Distinctness, Expr, FromClause, JoinConstraint, JoinOperator, JoinType, Name, OneSelect,
    Operator, ResultColumn, Select, SelectTable, SortedColumn, UnaryOperator,
};

/// A FROM item's column: its name, lowercase, when sqlive knows the name
/// SQLite gives it, and its traits.
#[derive(Clone)]
pub(crate) struct ItemColumn {
    name: Option<String>,
    traits: Traits,
}

/// What a FROM item reads rows from.
#[derive(Clone)]
enum Source<'a> {
    /// A table of the main schema, by its stored name, with the columns,
    /// lowercase, that hold equal values only where they are the same: its
    /// stored columns of BINARY collation whose declared type gives them an
    /// affinity other than BLOB.
    Table { name: String, exact: BTreeSet<String> },
    /// A FROM subquery.
    Subquery(&'a Select<'a>),
    /// A CTE, a view or a table function, whose rows nothing tells apart.
    Other,
}

#[derive(Clone)]
pub(crate) struct Item<'a> {
    /// The name the statement qualifies its columns by, as written; none for
    /// a subquery with no alias.
    qualifier: Option<&'a str>,
    source: Source<'a>,
    /// Its columns, when sqlive knows them.
    columns: Option<Vec<ItemColumn>>,
}

/// The names of one enclosing SELECT's FROM items, as a SELECT inside it can
/// name them: each item's qualifier, as written, with its columns when sqlive
/// knows them. `None` for a FROM sqlive can't read, where a name stops.
pub(crate) type OuterItems = Option<Vec<(Option<String>, Option<Vec<ItemColumn>>)>>;

/// What a name that could be a column names.
enum Named {
    /// A column, lowercase, of one of the FROM's own items, by its place.
    Own(usize, String),
    /// A column of an enclosing SELECT's item: one value for each row of
    /// that SELECT.
    Outer(Traits),
    Unfollowed,
}

/// An equality a condition holds between a column of an item and a value the
/// other items and the SELECTs around give.
struct Equality {
    item: usize,
    column: String,
    /// The FROM's own items the value depends on.
    needs: BTreeSet<usize>,
    /// The item whose LEFT JOIN's ON holds it, or `None` for the WHERE's and
    /// an inner join's ON's, which hold of every row.
    on: Option<usize>,
    /// The collation SQLite compares the two by.
    collation: String,
}

/// A term a sort compares: a column of an item, and the collation it compares
/// it by.
struct Sorted {
    item: usize,
    column: String,
    collation: String,
}

/// A LEFT JOIN, by the item it adds, with the items its ON or USING depends
/// on.
struct LeftJoin {
    item: usize,
    needs: BTreeSet<usize>,
}

/// A FROM as the rule reads it.
struct Rows<'a> {
    items: Vec<Item<'a>>,
    equalities: Vec<Equality>,
    left: Vec<LeftJoin>,
}

/// A column of a subquery: its name, the FROM item's column it passes on when
/// it is one, and its traits.
struct Output {
    name: Option<String>,
    passes: Option<(usize, String)>,
    traits: Traits,
}

/// Reads what tells a SELECT's rows apart, from the connection's schema.
pub(crate) struct Telling<'c> {
    pub(crate) connection: &'c Connection,
    pub(crate) schema: &'c MainSchema,
    /// The connection's functions, which tell an aggregate call from a scalar
    /// one.
    pub(crate) functions: &'c Functions,
}

/// Each name `expr` could resolve to a column, in it and in every SELECT
/// inside it, as its qualifier, as written, and its column, lowercase.
fn names_in(
    schema: &MainSchema,
    expr: &Expr<'_>,
) -> rusqlite::Result<Vec<(Option<String>, String)>> {
    #[derive(Default)]
    struct Names(Vec<(Option<String>, String)>);
    impl Visit for Names {
        fn name(&mut self, expr: &Expr<'_>) {
            if let Some((qualifier, column)) = qualified_name(expr) {
                self.0.push((qualifier.map(str::to_owned), column));
            }
        }
    }
    let mut walk = Walk::new(schema, Names::default());
    walk.expr(expr)?;
    Ok(walk.visit.0)
}

/// The qualifier, as written, and the column, lowercase, of a name that could
/// be a column of the main schema.
fn qualified_name<'a>(expr: &'a Expr<'a>) -> Option<(Option<&'a str>, String)> {
    let (qualifier, name) = match unparenthesized(expr) {
        Expr::Id(name) => (None, name.0),
        Expr::Qualified(table, name) => (Some(table.0), name.0),
        Expr::DoublyQualified(db, table, name) if *db == "main" => (Some(table.0), name.0),
        _ => return None,
    };
    Some((qualifier, crate::sql::name(name).to_ascii_lowercase()))
}

/// Whether `columns` has one named `column`.
fn lists(columns: &[ItemColumn], column: &str) -> bool {
    columns.iter().any(|listed| listed.name.as_deref() == Some(column))
}

/// The traits of the column `column` among `columns`; a rowid's where no
/// column has its name.
fn column_traits(columns: &[ItemColumn], column: &str) -> Traits {
    match columns.iter().find(|listed| listed.name.as_deref() == Some(column)) {
        Some(listed) => listed.traits.clone(),
        None => Traits {
            affinity: Carried::Of(Affinity::Numeric),
            collation: Carried::Of(BINARY.to_owned()),
        },
    }
}

/// What the name `expr` names among the FROM's own `items` and, past them,
/// the enclosing SELECTs' `outer`, the innermost last.
fn resolve(items: &[Item<'_>], outer: &[OuterItems], expr: &Expr<'_>) -> Named {
    let Some((qualifier, column)) = qualified_name(expr) else { return Named::Unfollowed };
    let rowid = ROWID_NAMES.contains(&column.as_str());
    if let Some(qualifier) = qualifier {
        let is = |named: Option<&str>| named.is_some_and(|named| Name(named) == Name(qualifier));
        if let Some(place) = items.iter().position(|item| is(item.qualifier)) {
            return Named::Own(place, column);
        }
        for level in outer.iter().rev() {
            let Some(level) = level else { return Named::Unfollowed };
            if let Some((_, columns)) = level.iter().find(|(named, _)| is(named.as_deref())) {
                return match columns {
                    Some(columns) if rowid || lists(columns, &column) => {
                        Named::Outer(column_traits(columns, &column))
                    }
                    _ => Named::Unfollowed,
                };
            }
        }
        return Named::Unfollowed;
    }
    let mut found = Vec::new();
    for (place, item) in items.iter().enumerate() {
        match &item.columns {
            // An item whose columns sqlive doesn't know could hold the name.
            None => return Named::Unfollowed,
            Some(columns) if lists(columns, &column) => found.push(place),
            Some(_) => {}
        }
    }
    match found[..] {
        [place] => return Named::Own(place, column),
        // A rowid's name that no column has names the rowid of the one table.
        [] if rowid && items.len() == 1 => return Named::Own(0, column),
        [] if !rowid => {}
        _ => return Named::Unfollowed,
    }
    for level in outer.iter().rev() {
        let Some(level) = level else { return Named::Unfollowed };
        let mut found = Vec::new();
        for (_, columns) in level {
            let Some(columns) = columns else { return Named::Unfollowed };
            if lists(columns, &column) {
                found.push(column_traits(columns, &column));
            }
        }
        match &found[..] {
            [] => {}
            [traits] => return Named::Outer(traits.clone()),
            _ => return Named::Unfollowed,
        }
    }
    Named::Unfollowed
}

impl Telling<'_> {
    /// Whether `select`'s ORDER BY tells every two of its rows apart, among
    /// the SELECTs `outer` around it. The query is one SELECT, and either it
    /// calls an aggregate function in its result columns with no GROUP BY and
    /// no window, so it returns one row, or it has no DISTINCT, neither
    /// groups nor calls an aggregate or window function, and its ORDER BY and
    /// the equalities of its conditions tell the rows it joins apart.
    pub(crate) fn query_tells_apart(
        &self,
        scope: &crate::sql_walk::Scope<'_>,
        outer: &[OuterItems],
        select: &Select<'_>,
    ) -> rusqlite::Result<bool> {
        if select.body.compounds.is_some() {
            return Ok(false);
        }
        let OneSelect::Select {
            distinctness,
            columns,
            from,
            where_clause,
            group_by: None,
            having,
            window_clause: None,
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
            || *distinctness == Some(Distinctness::Distinct)
            || having.is_some()
            || sorted.iter().any(|part| part.aggregates(self.functions))
        {
            return Ok(false);
        }
        let Some(rows) = self.rows(scope, outer, from.as_ref(), *where_clause, columns)? else {
            return Ok(false);
        };
        let terms = order_by.iter().filter_map(|term| match order_term(columns, &term.expr) {
            OrderTerm::Position(expr) | OrderTerm::Alias(_, expr) | OrderTerm::Expr(expr) => {
                Some(expr)
            }
            OrderTerm::Unknown => None,
        });
        let sorted = self.sorted(&rows.items, outer, terms);
        self.all_told(scope, outer, &rows, &sorted)
    }

    /// Whether the terms `order_by`, which name no result column by position
    /// or alias, tell every two rows of the SELECT of `from` and
    /// `where_clause` apart: a window's or an aggregate call's ORDER BY.
    pub(crate) fn core_tells_apart(
        &self,
        scope: &crate::sql_walk::Scope<'_>,
        outer: &[OuterItems],
        core: &OneSelect<'_>,
        order_by: &[SortedColumn<'_>],
    ) -> rusqlite::Result<bool> {
        let OneSelect::Select { columns, from, where_clause, .. } = core else { return Ok(false) };
        let Some(rows) = self.rows(scope, outer, from.as_ref(), *where_clause, columns)? else {
            return Ok(false);
        };
        let sorted = self.sorted(&rows.items, outer, order_by.iter().map(|term| &term.expr));
        self.all_told(scope, outer, &rows, &sorted)
    }

    /// The columns of the items that the sort terms `terms` compare.
    fn sorted<'e>(
        &self,
        items: &[Item<'_>],
        outer: &[OuterItems],
        terms: impl Iterator<Item = &'e Expr<'e>>,
    ) -> Vec<Sorted> {
        terms
            .filter_map(|term| {
                let (expr, collated) = match unparenthesized(term) {
                    Expr::Collate(expr, collation) => (*expr, Some(crate::sql::name(collation))),
                    expr => (expr, None),
                };
                let Named::Own(item, column) = resolve(items, outer, expr) else { return None };
                let collation = match collated {
                    Some(collation) => collation.to_owned(),
                    None => match column_traits(items[item].columns.as_ref()?, &column).collation {
                        Carried::Of(collation) => collation,
                        Carried::Nothing | Carried::Unfollowed => return None,
                    },
                };
                Some(Sorted { item, column, collation })
            })
            .collect()
    }

    /// The FROM `from` of a SELECT among `outer`, with the equalities of its
    /// ONs and of `where_clause`, whose result columns are `columns`; `None`
    /// for a FROM the rule doesn't read: one with a join in parentheses or a
    /// RIGHT or FULL JOIN.
    fn rows<'a>(
        &self,
        scope: &crate::sql_walk::Scope<'_>,
        outer: &[OuterItems],
        from: Option<&'a FromClause<'a>>,
        where_clause: Option<&'a Expr<'a>>,
        columns: &[ResultColumn<'_>],
    ) -> rusqlite::Result<Option<Rows<'a>>> {
        let Some(items) = self.items(scope, outer, from)? else { return Ok(None) };
        let aliases: Vec<String> = columns
            .iter()
            .filter_map(|column| match column {
                ResultColumn::Expr(_, Some(alias)) => Some(crate::sql::alias_name(alias)),
                _ => None,
            })
            .collect();
        let mut rows = Rows { items, equalities: Vec::new(), left: Vec::new() };
        for (position, join) in from.iter().flat_map(|from| from.joins.iter().flatten()).enumerate()
        {
            // The item the join adds, which sits after the FROM's first.
            let item = position + 1;
            let kind = match join.operator {
                JoinOperator::TypedJoin(Some(kind)) => kind,
                JoinOperator::TypedJoin(None) | JoinOperator::Comma => JoinType::empty(),
            };
            if kind.contains(JoinType::RIGHT) {
                return Ok(None);
            }
            let on = kind.contains(JoinType::LEFT).then_some(item);
            let mut needs = BTreeSet::new();
            match &join.constraint {
                Some(JoinConstraint::On(expr)) => {
                    match self.needs(&rows.items, &aliases, expr)? {
                        Some(found) => needs = found,
                        None => needs.extend(0..rows.items.len()),
                    }
                    self.equalities(&mut rows, outer, &aliases, expr, on)?;
                }
                Some(JoinConstraint::Using(names)) => {
                    for name in names.iter() {
                        let column = crate::sql::name(name.0).to_ascii_lowercase();
                        // SQLite compares the joined item's column with that
                        // of the leftmost item before it that has one, by that
                        // item's collation.
                        let leftmost = rows.items[..item].iter().position(|before| {
                            before.columns.as_ref().is_none_or(|columns| lists(columns, &column))
                        });
                        let Some(leftmost) = leftmost else { continue };
                        needs.insert(leftmost);
                        let sides = (&rows.items[leftmost].columns, &rows.items[item].columns);
                        let (Some(first), Some(joined)) = sides else { continue };
                        let first = column_traits(first, &column);
                        let joined = column_traits(joined, &column);
                        let Some(collation) = comparison_collation(&first, &joined) else {
                            continue;
                        };
                        for (keyed, mine, other, theirs) in
                            [(item, &joined, leftmost, &first), (leftmost, &first, item, &joined)]
                        {
                            let Carried::Of(affinity) = mine.affinity else { continue };
                            if keeps_values(affinity, &theirs.affinity, true) {
                                rows.equalities.push(Equality {
                                    item: keyed,
                                    column: column.clone(),
                                    needs: BTreeSet::from([other]),
                                    on,
                                    collation: collation.clone(),
                                });
                            }
                        }
                    }
                }
                None => {}
            }
            if on.is_some() {
                // A NATURAL JOIN compares columns it doesn't name.
                if kind.contains(JoinType::NATURAL) {
                    needs.extend(0..item);
                }
                rows.left.push(LeftJoin { item, needs });
            }
        }
        if let Some(expr) = where_clause {
            self.equalities(&mut rows, outer, &aliases, expr, None)?;
        }
        Ok(Some(rows))
    }

    /// The FROM items of `from`, among `outer`; `None` when a join is in
    /// parentheses.
    pub(crate) fn items<'a>(
        &self,
        scope: &crate::sql_walk::Scope<'_>,
        outer: &[OuterItems],
        from: Option<&'a FromClause<'a>>,
    ) -> rusqlite::Result<Option<Vec<Item<'a>>>> {
        let mut items = Vec::new();
        let listed = from.into_iter().flat_map(|from| {
            from.select.into_iter().chain(from.joins.iter().flatten().map(|join| &join.table))
        });
        let alias = |alias: &Option<As<'a>>| match alias {
            Some(As::As(alias) | As::Elided(alias)) => Some(alias.0),
            None => None,
        };
        for item in listed {
            items.push(match item {
                SelectTable::Table(name, given, _) => {
                    let qualifier = Some(alias(given).unwrap_or(name.name.0));
                    match scope.object(name, &["table", "virtual"]) {
                        // A CTE's name names no table.
                        Some(table) => {
                            let named = alias(given).unwrap_or(name.name.0);
                            let (source, columns) = self.table_item(named, table.to_owned())?;
                            Item { qualifier, columns: Some(columns), source }
                        }
                        None => {
                            let columns = match scope.object(name, &["view"]) {
                                Some(view) => Some(
                                    self.schema
                                        .columns(view)?
                                        .iter()
                                        .map(|column| ItemColumn {
                                            name: Some(column.name.to_ascii_lowercase()),
                                            traits: Traits::UNFOLLOWED,
                                        })
                                        .collect(),
                                ),
                                None => None,
                            };
                            Item { qualifier, columns, source: Source::Other }
                        }
                    }
                }
                SelectTable::Select(select, given) => {
                    let columns = self.outputs(scope, outer, select)?.map(|outputs| {
                        outputs
                            .into_iter()
                            .map(|output| ItemColumn { name: output.name, traits: output.traits })
                            .collect()
                    });
                    Item { qualifier: alias(given), columns, source: Source::Subquery(select) }
                }
                SelectTable::TableCall(name, _, given) => Item {
                    qualifier: Some(alias(given).unwrap_or(name.name.0)),
                    columns: None,
                    source: Source::Other,
                },
                SelectTable::Sub(..) => return Ok(None),
            });
        }
        Ok(Some(items))
    }

    /// The table `table`, which the statement names by `qualifier`, as a
    /// FROM item reads it: its columns' traits, as `from_item::columns` reads
    /// them, and which of them hold equal values only where they are the
    /// same.
    fn table_item(
        &self,
        qualifier: &str,
        table: String,
    ) -> rusqlite::Result<(Source<'static>, Vec<ItemColumn>)> {
        let listed = crate::from_item::columns(
            self.connection,
            self.schema,
            self.functions,
            qualifier,
            &table,
            true,
        )?;
        let Columns::Listed(listed) = listed else { panic!("a table's columns are listed") };
        let mut exact = BTreeSet::new();
        let columns = listed
            .into_iter()
            .map(|Column { name, value }| {
                if let ColumnValue::Compared(Affinity::Numeric | Affinity::Text) = value {
                    exact.extend(name.clone());
                }
                let compared = |affinity, collation: &str| Traits {
                    affinity: Carried::Of(affinity),
                    collation: Carried::Of(collation.to_owned()),
                };
                let traits = match value {
                    ColumnValue::Compared(affinity) => compared(affinity, BINARY),
                    ColumnValue::Generated(expression) => compared(expression.affinity, BINARY),
                    ColumnValue::Collated(affinity, collation) => compared(affinity, &collation),
                    ColumnValue::Unknown | ColumnValue::Volatile | ColumnValue::Hidden => {
                        Traits::UNFOLLOWED
                    }
                };
                ItemColumn { name, traits }
            })
            .collect();
        Ok((Source::Table { name: table, exact }, columns))
    }

    /// The columns the subquery `select` returns, as its first SELECT names
    /// them; `None` when sqlive doesn't know how many a `*` adds, or the
    /// subquery has a WITH of its own, whose names this doesn't resolve.
    fn outputs(
        &self,
        scope: &crate::sql_walk::Scope<'_>,
        outer: &[OuterItems],
        select: &Select<'_>,
    ) -> rusqlite::Result<Option<Vec<Output>>> {
        if select.with.is_some() {
            return Ok(None);
        }
        let OneSelect::Select { columns, from, .. } = &select.body.select else { return Ok(None) };
        let Some(items) = self.items(scope, outer, from.as_ref())? else { return Ok(None) };
        let using = from.iter().flat_map(|from| from.joins.iter().flatten()).any(|join| {
            matches!(join.constraint, Some(JoinConstraint::Using(_)))
                || matches!(
                    join.operator,
                    JoinOperator::TypedJoin(Some(kind)) if kind.contains(JoinType::NATURAL)
                )
        });
        // A compound compares its rows by what its arms give together.
        let compound = select.body.compounds.is_some();
        let mut outputs = Vec::new();
        for column in columns.iter() {
            let starred: Vec<usize> = match column {
                ResultColumn::Expr(expr, given) => {
                    let passes = match resolve(&items, outer, expr) {
                        Named::Own(item, column) => Some((item, column)),
                        Named::Outer(_) | Named::Unfollowed => None,
                    };
                    let name = match given {
                        Some(alias) => Some(crate::sql::alias_name(alias)),
                        None => qualified_name(expr).map(|(_, column)| column),
                    };
                    let traits = self.traits(&items, outer, expr);
                    outputs.push(Output { name, passes, traits });
                    continue;
                }
                // A `*` over a USING or NATURAL join leaves out columns.
                ResultColumn::Star | ResultColumn::TableStar(_) if using => return Ok(None),
                ResultColumn::Star => (0..items.len()).collect(),
                ResultColumn::TableStar(table) => items
                    .iter()
                    .position(|item| {
                        item.qualifier.is_some_and(|named| Name(named) == Name(table.0))
                    })
                    .into_iter()
                    .collect(),
            };
            if starred.is_empty() {
                return Ok(None);
            }
            for place in starred {
                let Some(columns) = &items[place].columns else { return Ok(None) };
                outputs.extend(columns.iter().map(|column| Output {
                    name: column.name.clone(),
                    passes: column.name.clone().map(|name| (place, name)),
                    traits: column.traits.clone(),
                }));
            }
        }
        if compound {
            for output in &mut outputs {
                output.traits = Traits::UNFOLLOWED;
            }
        }
        Ok(Some(outputs))
    }

    /// What a comparison takes from `expr`, among the FROM's `items` and
    /// `outer`.
    fn traits(&self, items: &[Item<'_>], outer: &[OuterItems], expr: &Expr<'_>) -> Traits {
        let mut found = Vec::new();
        parts(expr, &mut found);
        if found.iter().any(|part| matches!(part, Part::Collation)) {
            return Traits::UNFOLLOWED;
        }
        match unparenthesized(expr) {
            Expr::Id(_) | Expr::Qualified(..) | Expr::DoublyQualified(..) => {
                match resolve(items, outer, expr) {
                    Named::Own(item, column) => match &items[item].columns {
                        Some(columns) => column_traits(columns, &column),
                        None => Traits::UNFOLLOWED,
                    },
                    Named::Outer(traits) => traits,
                    Named::Unfollowed => Traits::UNFOLLOWED,
                }
            }
            Expr::Cast { expr, type_name } => Traits {
                affinity: match type_name {
                    Some(type_name) => Carried::Of(Affinity::declared(type_name.name)),
                    None => Carried::Unfollowed,
                },
                collation: self.traits(items, outer, expr).collation,
            },
            Expr::Unary(UnaryOperator::Positive, expr) => Traits {
                affinity: Carried::Nothing,
                collation: self.traits(items, outer, expr).collation,
            },
            // A subquery's value carries its result column's affinity and
            // collation.
            Expr::Subquery(_) => Traits::UNFOLLOWED,
            _ => Traits::VALUE,
        }
    }

    /// The FROM's own items that `expr` depends on, a name in a SELECT inside
    /// it taken as every item's that could hold it; `None` when a name could
    /// be one of the SELECT's result columns' `aliases`, which stands for an
    /// expression this doesn't follow.
    fn needs(
        &self,
        items: &[Item<'_>],
        aliases: &[String],
        expr: &Expr<'_>,
    ) -> rusqlite::Result<Option<BTreeSet<usize>>> {
        let mut needs = BTreeSet::new();
        for (qualifier, column) in names_in(self.schema, expr)? {
            let mut found = false;
            for (place, item) in items.iter().enumerate() {
                let holds = match &qualifier {
                    Some(qualifier) => {
                        item.qualifier.is_some_and(|named| Name(named) == Name(qualifier))
                    }
                    None => item.columns.as_ref().is_none_or(|columns| lists(columns, &column)),
                };
                if holds {
                    needs.insert(place);
                    found = true;
                }
            }
            if !found && qualifier.is_none() {
                if ROWID_NAMES.contains(&column.as_str()) {
                    needs.extend(0..items.len());
                } else if aliases.contains(&column) {
                    return Ok(None);
                }
            }
        }
        Ok(Some(needs))
    }

    /// Add to `rows` the equalities of the conditions `expr` joins with AND,
    /// in the ON of the LEFT JOIN that adds item `on`, or in the WHERE or an
    /// inner join's ON.
    fn equalities(
        &self,
        rows: &mut Rows<'_>,
        outer: &[OuterItems],
        aliases: &[String],
        expr: &Expr<'_>,
        on: Option<usize>,
    ) -> rusqlite::Result<()> {
        let mut conditions = Vec::new();
        conjuncts(expr, &mut conditions);
        for condition in conditions {
            let Expr::Binary(left, Operator::Equals, right) = condition else { continue };
            let left_traits = self.traits(&rows.items, outer, left);
            let right_traits = self.traits(&rows.items, outer, right);
            let Some(collation) = comparison_collation(&left_traits, &right_traits) else {
                continue;
            };
            for (keyed, mine, value, theirs) in [
                (left, &left_traits, right, &right_traits),
                (right, &right_traits, left, &left_traits),
            ] {
                let Named::Own(item, column) = resolve(&rows.items, outer, keyed) else { continue };
                let Carried::Of(affinity) = mine.affinity else { continue };
                let named = matches!(
                    unparenthesized(value),
                    Expr::Id(_) | Expr::Qualified(..) | Expr::DoublyQualified(..)
                );
                if !keeps_values(affinity, &theirs.affinity, named) {
                    continue;
                }
                let Some(needs) = self.needs(&rows.items, aliases, value)? else { continue };
                rows.equalities.push(Equality {
                    item,
                    column,
                    needs,
                    on,
                    collation: collation.clone(),
                });
            }
        }
        Ok(())
    }

    /// Whether each item of `rows` is, in turn, one that adds one row at most
    /// to each row of those before it, among rows that tie in `sorted`.
    fn all_told(
        &self,
        scope: &crate::sql_walk::Scope<'_>,
        outer: &[OuterItems],
        rows: &Rows<'_>,
        sorted: &[Sorted],
    ) -> rusqlite::Result<bool> {
        let mut known = BTreeSet::new();
        loop {
            let before = known.len();
            for item in 0..rows.items.len() {
                if !known.contains(&item)
                    && self.determines(scope, outer, rows, sorted, &known, item)?
                {
                    known.insert(item);
                }
            }
            if known.len() == before {
                return Ok(known.len() == rows.items.len());
            }
        }
    }

    /// Whether item `item` of `rows` adds one row at most, or the one NULL
    /// row of a LEFT JOIN, to each row of the items `known`, among rows that
    /// tie in `sorted`.
    fn determines(
        &self,
        scope: &crate::sql_walk::Scope<'_>,
        outer: &[OuterItems],
        rows: &Rows<'_>,
        sorted: &[Sorted],
        known: &BTreeSet<usize>,
        item: usize,
    ) -> rusqlite::Result<bool> {
        let equalities: Vec<&Equality> = rows
            .equalities
            .iter()
            .filter(|equality| {
                equality.item == item
                    && equality.on.is_none_or(|on| on == item)
                    && equality.needs.is_subset(known)
            })
            .collect();
        // Whether the items before decide whether a LEFT JOIN finds the item
        // a row, so no two rows differ only in whether it did. A column the
        // sort compares or the WHERE fixes tells them apart too.
        let decided = rows
            .left
            .iter()
            .find(|left| left.item == item)
            .map(|left| left.needs.iter().all(|need| *need == item || known.contains(need)));
        match &rows.items[item].source {
            Source::Table { name: table, .. } => {
                let not_null = not_null_columns(self.schema, table)?;
                for key in unique_keys(self.schema, table)? {
                    let mut covered = true;
                    let mut anchored = false;
                    for (column, key_collation) in &key {
                        let is_key = |named: &str| -> rusqlite::Result<bool> {
                            Ok(match column.as_str() {
                                ROWID => is_rowid(self.schema, table, named)?,
                                column => named == column,
                            })
                        };
                        let mut sorted_by = false;
                        for term in sorted.iter().filter(|term| term.item == item) {
                            // A rowid holds an integer, which every collation
                            // compares alike.
                            if is_key(&term.column)?
                                && (column == ROWID
                                    || at_least_as_fine(&term.collation, key_collation)
                                        && not_null.contains(column))
                            {
                                sorted_by = true;
                            }
                        }
                        let mut inner = false;
                        let mut equated = false;
                        for equality in &equalities {
                            if is_key(&equality.column)?
                                && at_least_as_fine(&equality.collation, key_collation)
                            {
                                equated = true;
                                inner |= equality.on.is_none();
                            }
                        }
                        covered &= sorted_by || equated;
                        anchored |= sorted_by || inner;
                    }
                    if covered && decided.is_none_or(|decided| anchored || decided) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Source::Subquery(select) => {
                let fixed: Vec<(&str, &str)> = equalities
                    .iter()
                    .map(|equality| (equality.column.as_str(), equality.collation.as_str()))
                    .collect();
                let anchored = equalities.iter().any(|equality| equality.on.is_none());
                Ok(self.one_row(scope, outer, select, &fixed)?
                    && decided.is_none_or(|decided| anchored || decided))
            }
            Source::Other => Ok(false),
        }
    }

    /// Whether the subquery `select` returns one row at most for any values,
    /// none NULL, of its columns `fixed`, each compared by its collation: it
    /// is one SELECT that aggregates with no GROUP BY, or groups by columns
    /// that `fixed` returns, or neither aggregates nor calls a window
    /// function, and equalities with the columns `fixed` returns tell its
    /// rows apart.
    fn one_row(
        &self,
        scope: &crate::sql_walk::Scope<'_>,
        outer: &[OuterItems],
        select: &Select<'_>,
        fixed: &[(&str, &str)],
    ) -> rusqlite::Result<bool> {
        if select.body.compounds.is_some() {
            return Ok(false);
        }
        let OneSelect::Select { columns, from, where_clause, group_by, having, .. } =
            &select.body.select
        else {
            return Ok(false);
        };
        let Some(outputs) = self.outputs(scope, outer, select)? else { return Ok(false) };
        let mut found = Vec::new();
        for column in columns.iter() {
            if let ResultColumn::Expr(expr, _) = column {
                parts(expr, &mut found);
            }
        }
        for expr in having.iter().copied() {
            parts(expr, &mut found);
        }
        for term in select.order_by.unwrap_or_default() {
            parts(&term.expr, &mut found);
        }
        let windowed = found.iter().any(Part::windowed);
        let aggregated =
            group_by.is_some() || found.iter().any(|part| part.aggregates(self.functions));
        if aggregated && group_by.is_none() && !windowed {
            return Ok(true);
        }
        let Some(mut rows) = self.rows(scope, outer, from.as_ref(), *where_clause, columns)? else {
            return Ok(false);
        };
        // The FROM item's column each fixed column passes on, with the
        // collation the equality compares it by.
        let fixed = fixed.iter().filter_map(|(name, collation)| {
            let output = outputs.iter().find(|output| output.name.as_deref() == Some(name))?;
            Some((output.passes.clone()?, *collation))
        });
        if let Some(terms) = group_by {
            let fixed: Vec<_> = fixed.collect();
            for term in terms.iter() {
                let (OrderTerm::Position(expr) | OrderTerm::Alias(_, expr) | OrderTerm::Expr(expr)) =
                    order_term(columns, term)
                else {
                    return Ok(false);
                };
                let Named::Own(item, column) = resolve(&rows.items, outer, expr) else {
                    return Ok(false);
                };
                // A group's key shows one value only where values that
                // compare equal are the same, and then every collation an
                // equality compares it by is as fine as the grouping's.
                if !self.compares_exactly(&rows.items, item, &column)?
                    || !fixed.iter().any(|((place, passed), collation)| {
                        (*place, passed) == (item, &column) && at_least_as_fine(collation, BINARY)
                    })
                {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        if aggregated || windowed {
            return Ok(false);
        }
        for ((item, column), collation) in fixed {
            rows.equalities.push(Equality {
                item,
                column,
                needs: BTreeSet::new(),
                on: None,
                collation: collation.to_owned(),
            });
        }
        self.all_told(scope, outer, &rows, &[])
    }

    /// Whether two values that the column `column` of item `item` holds that
    /// compare equal are the same value: a table's rowid, or its stored
    /// column of BINARY collation whose declared type gives it an affinity
    /// other than BLOB, which stores an integer and a real that compare equal
    /// as one of them. An untyped column can hold both `1` and `1.0`, and a
    /// NOCASE one both `'Abc'` and `'ABC'`.
    pub(crate) fn compares_exactly(
        &self,
        items: &[Item<'_>],
        item: usize,
        column: &str,
    ) -> rusqlite::Result<bool> {
        let Source::Table { name, exact } = &items[item].source else { return Ok(false) };
        Ok(exact.contains(column) || is_rowid(self.schema, name, column)?)
    }

    /// The FROM item, by its place in `items`, and the column, lowercase,
    /// that the name `expr` names among them.
    pub(crate) fn own_column(
        items: &[Item<'_>],
        outer: &[OuterItems],
        expr: &Expr<'_>,
    ) -> Option<(usize, String)> {
        match resolve(items, outer, expr) {
            Named::Own(item, column) => Some((item, column)),
            Named::Outer(_) | Named::Unfollowed => None,
        }
    }

    /// The names a SELECT inside the one of `items` can give them.
    pub(crate) fn outer_items(items: Option<Vec<Item<'_>>>) -> OuterItems {
        items.map(|items| {
            items
                .into_iter()
                .map(|item| (item.qualifier.map(str::to_owned), item.columns))
                .collect()
        })
    }
}
