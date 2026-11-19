//! The statements the differential's generators write: SELECT queries over
//! the shared tables, written as SQL and as the runner's JSON tree, and
//! whether SQLite may pick other rows than the model does.

use serde_json::{json, Value as Json};

/// One SELECT statement, or a query inside one.
#[derive(Clone, Default)]
pub(super) struct Query {
    pub(super) with: Vec<(String, Query)>,
    pub(super) arms: Vec<Core>,
    /// The operator before each arm after the first: `UNION ALL`, `UNION`,
    /// `EXCEPT` or `INTERSECT`.
    pub(super) ops: Vec<&'static str>,
    /// Result column positions, each descending when `true`.
    pub(super) order: Vec<(usize, bool)>,
    /// Whether the ORDER BY names each term's column instead of its position.
    pub(super) order_named: bool,
    pub(super) limit: Option<Limit>,
}

/// A LIMIT, with the OFFSET SQL writes only after one.
#[derive(Clone, Copy)]
pub(super) struct Limit {
    pub(super) count: u32,
    pub(super) offset: Option<u32>,
}

/// A LIMIT of `count` rows with no OFFSET.
pub(super) fn limit(count: u32) -> Option<Limit> {
    Some(Limit { count, offset: None })
}

#[derive(Clone, Default)]
pub(super) struct Core {
    pub(super) from: Vec<Item>,
    pub(super) conditions: Vec<Cond>,
    pub(super) select: Vec<Out>,
    pub(super) group: Vec<Expr>,
    /// `SELECT DISTINCT`.
    pub(super) distinct: bool,
}

#[derive(Clone)]
pub(super) struct Item {
    pub(super) alias: String,
    pub(super) source: Source,
    pub(super) join: Join,
    pub(super) on: Vec<Cond>,
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Join {
    /// The first item, or one after a comma.
    Comma,
    Inner,
    Left,
    /// `JOIN ... USING (columns)`, or `LEFT JOIN` when `left`: the model reads
    /// it as the equality of each column with the leftmost item before it
    /// that has the column.
    Using {
        left: bool,
        columns: &'static [&'static str],
    },
    Natural,
}

#[derive(Clone)]
pub(super) enum Source {
    Table(&'static str),
    View(&'static str),
    Cte(String),
    Query(Box<Query>),
}

#[derive(Clone)]
pub(super) enum Expr {
    Col(String, &'static str),
    Lit(Option<i64>),
    Add(Box<Expr>, Box<Expr>),
    /// `max(l, r)`: SQLite's scalar max, which is no aggregate.
    Greatest(Box<Expr>, Box<Expr>),
    Sub(Box<Query>),
    /// `coalesce(l, r)`.
    Coalesce(Box<Expr>, Box<Expr>),
}

#[derive(Clone)]
pub(super) enum Cond {
    Cmp(&'static str, Expr, Expr),
    Null(Expr, bool),
    InList(Expr, Vec<Expr>),
    InQuery(Expr, Box<Query>, bool),
    Exists(Box<Query>, bool),
    /// A row value compared with a subquery's row: `=`, with the subquery
    /// first when `first`, or IN when `member`; NOT around it when `not`.
    RowSub {
        values: Vec<Expr>,
        query: Box<Query>,
        member: bool,
        first: bool,
        not: bool,
    },
    /// `(l1, ..., ln) = (r1, ..., rn)`.
    RowEq(Vec<Expr>, Vec<Expr>),
    And(Box<Cond>, Box<Cond>),
    Or(Box<Cond>, Box<Cond>),
    Not(Box<Cond>),
}

#[derive(Clone)]
pub(super) enum Out {
    Expr(Expr, Option<String>),
    Count(Option<String>),
    Max(Expr, Option<String>),
    /// `CAST(group_concat(e, '' ORDER BY ...) AS INTEGER)`, each term
    /// descending when `true`: the values joined in order, read back as an
    /// integer, which the model's values are.
    Concat(Expr, Vec<(Expr, bool)>, Option<String>),
    /// `row_number() OVER (PARTITION BY ... ORDER BY ...)`, each ORDER BY
    /// term descending when `true`.
    RowNumber(Vec<Expr>, Vec<(Expr, bool)>, Option<String>),
}

pub(super) fn col(alias: &str, column: &'static str) -> Expr {
    Expr::Col(alias.to_owned(), column)
}

pub(super) fn lit(value: i64) -> Expr {
    Expr::Lit(Some(value))
}

pub(super) fn cmp(op: &'static str, left: Expr, right: Expr) -> Cond {
    Cond::Cmp(op, left, right)
}

pub(super) fn eq(left: Expr, right: Expr) -> Cond {
    Cond::Cmp("=", left, right)
}

pub(super) fn out(e: Expr) -> Out {
    Out::Expr(e, None)
}

pub(super) fn item(alias: &str, source: Source, join: Join, on: Vec<Cond>) -> Item {
    Item { alias: alias.to_owned(), source, join, on }
}

pub(super) fn table(alias: &str, name: &'static str) -> Item {
    item(alias, Source::Table(name), Join::Comma, Vec::new())
}

/// A query of one core.
pub(super) fn select(core: Core) -> Query {
    Query { arms: vec![core], ..Query::default() }
}

impl Query {
    pub(super) fn sql(&self) -> String {
        let mut sql = String::new();
        if !self.with.is_empty() {
            let ctes: Vec<String> = self
                .with
                .iter()
                .map(|(name, query)| format!("{name} AS ({})", query.sql()))
                .collect();
            sql.push_str(&format!("WITH {} ", ctes.join(", ")));
        }
        for (position, arm) in self.arms.iter().enumerate() {
            if position > 0 {
                sql.push_str(&format!(" {} ", self.ops[position - 1]));
            }
            sql.push_str(&arm.sql());
        }
        if !self.order.is_empty() {
            let terms: Vec<String> = self
                .order
                .iter()
                .map(|(position, descending)| {
                    let term = match (&self.arms[0].select[*position], self.order_named) {
                        (Out::Expr(e, _), true) => e.sql(),
                        _ => (position + 1).to_string(),
                    };
                    if *descending {
                        format!("{term} DESC")
                    } else {
                        term
                    }
                })
                .collect();
            sql.push_str(&format!(" ORDER BY {}", terms.join(", ")));
        }
        if let Some(Limit { count, offset }) = self.limit {
            sql.push_str(&format!(" LIMIT {count}"));
            if let Some(offset) = offset {
                sql.push_str(&format!(" OFFSET {offset}"));
            }
        }
        sql
    }

    pub(super) fn json(&self) -> Json {
        let with: Vec<Json> = self
            .with
            .iter()
            .map(|(name, query)| json!({"name": name, "query": query.json()}))
            .collect();
        let order: Vec<Json> = self
            .order
            .iter()
            .map(|(position, descending)| json!({"position": position, "desc": descending}))
            .collect();
        json!({
            "with": with,
            "arms": self.arms.iter().map(Core::json).collect::<Vec<_>>(),
            "ops": self.ops.iter().map(|op| op.to_ascii_lowercase()).collect::<Vec<_>>(),
            "order": order,
            "order_named": self.order_named,
            "limit": self.limit.map(|limit| limit.count),
            "offset": self.limit.and_then(|limit| limit.offset),
        })
    }

    /// Whether SQLite may pick other rows than the model does: a LIMIT
    /// anywhere in it breaks ties by an order neither states, and a scalar
    /// subquery returns whichever row SQLite reads first.
    pub(super) fn ambiguous(&self) -> bool {
        self.limit.is_some()
            || self.with.iter().any(|(_, query)| query.ambiguous())
            || self.arms.iter().any(Core::ambiguous)
    }
}

impl Core {
    fn sql(&self) -> String {
        let columns: Vec<String> = self.select.iter().map(Out::sql).collect();
        let distinct = if self.distinct { "DISTINCT " } else { "" };
        let mut sql = format!("SELECT {distinct}{}", columns.join(", "));
        for (position, item) in self.from.iter().enumerate() {
            let source = match &item.source {
                Source::Table(name) | Source::View(name) => (*name).to_owned(),
                Source::Cte(name) => name.clone(),
                Source::Query(query) => format!("({})", query.sql()),
            };
            let on = || {
                let conditions: Vec<String> = item.on.iter().map(Cond::sql).collect();
                if conditions.is_empty() {
                    String::new()
                } else {
                    format!(" ON {}", conditions.join(" AND "))
                }
            };
            match (position, item.join) {
                (0, _) => sql.push_str(&format!(" FROM {source} AS {}", item.alias)),
                (_, Join::Comma) => sql.push_str(&format!(", {source} AS {}", item.alias)),
                (_, Join::Inner) => {
                    sql.push_str(&format!(" JOIN {source} AS {}{}", item.alias, on()))
                }
                (_, Join::Left) => {
                    sql.push_str(&format!(" LEFT JOIN {source} AS {}{}", item.alias, on()));
                }
                (_, Join::Using { left, columns }) => {
                    sql.push_str(&format!(
                        " {}JOIN {source} AS {} USING ({})",
                        if left { "LEFT " } else { "" },
                        item.alias,
                        columns.join(", ")
                    ));
                }
                (_, Join::Natural) => {
                    sql.push_str(&format!(" NATURAL JOIN {source} AS {}", item.alias));
                }
            }
        }
        if !self.conditions.is_empty() {
            let conditions: Vec<String> = self.conditions.iter().map(Cond::sql).collect();
            sql.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
        }
        if !self.group.is_empty() {
            let terms: Vec<String> = self.group.iter().map(Expr::sql).collect();
            sql.push_str(&format!(" GROUP BY {}", terms.join(", ")));
        }
        sql
    }

    fn json(&self) -> Json {
        let from: Vec<Json> = self
            .from
            .iter()
            .map(|item| {
                let (left, using) = match item.join {
                    Join::Left => (true, &[][..]),
                    Join::Using { left, columns } => (left, columns),
                    _ => (false, &[][..]),
                };
                let mut entry = json!({
                    "alias": item.alias,
                    "left": left,
                    "using": using,
                    "on": item.on.iter().map(Cond::json).collect::<Vec<_>>(),
                    "whole": if item.join == Join::Natural { vec!["natural"] } else { Vec::new() },
                });
                let (key, value) = match &item.source {
                    Source::Table(name) => ("table", json!(name)),
                    Source::View(name) => ("view", json!(name)),
                    Source::Cte(name) => ("cte", json!(name)),
                    Source::Query(query) => ("query", query.json()),
                };
                entry[key] = value;
                entry
            })
            .collect();
        json!({
            "from": from,
            "where": self.conditions.iter().map(Cond::json).collect::<Vec<_>>(),
            "select": self.select.iter().map(Out::json).collect::<Vec<_>>(),
            "group": self.group.iter().map(Expr::json).collect::<Vec<_>>(),
            "distinct": self.distinct,
        })
    }

    fn ambiguous(&self) -> bool {
        self.from.iter().any(|item| match &item.source {
            Source::Query(query) => query.ambiguous(),
            _ => false,
        }) || self.conditions.iter().any(Cond::ambiguous)
            || self.select.iter().any(|column| match column {
                Out::Expr(e, _) | Out::Max(e, _) => e.ambiguous(),
                Out::Count(_) => false,
                Out::Concat(e, order, _) => {
                    e.ambiguous() || order.iter().any(|(sorted, _)| sorted.ambiguous())
                }
                Out::RowNumber(partition, order, _) => {
                    partition.iter().any(Expr::ambiguous)
                        || order.iter().any(|(sorted, _)| sorted.ambiguous())
                }
            })
            || self.from.iter().any(|item| item.on.iter().any(Cond::ambiguous))
            || self.follows_order()
    }

    /// Whether SQLite picks which row of a group a column outside its GROUP
    /// BY comes from, the order `group_concat` joins its values in, which of
    /// the values that compare equal max keeps, or the order a window
    /// numbers rows that its ORDER BY doesn't tell apart: the model reads
    /// its rows in another order.
    fn follows_order(&self) -> bool {
        let aggregates = !self.group.is_empty()
            || self
                .select
                .iter()
                .any(|column| !matches!(column, Out::Expr(..) | Out::RowNumber(..)));
        let rowids = |order: &[(Expr, bool)]| {
            self.from.iter().all(|item| {
                matches!(item.source, Source::Table(_))
                    && order.iter().any(|(sorted, _)| {
                        matches!(sorted, Expr::Col(alias, "id") if *alias == item.alias)
                    })
            })
        };
        self.select.iter().any(|column| match column {
            Out::Expr(e, _) => {
                aggregates
                    && !matches!(e, Expr::Lit(_))
                    && !self.group.iter().any(|g| g.sql() == e.sql())
            }
            Out::RowNumber(_, order, _) => aggregates || !rowids(order),
            Out::Concat(_, order, _) => !rowids(order),
            // The untyped `b` and the NOCASE `n` can hold values that
            // compare equal and differ (`super::TABLE_SQL`).
            Out::Max(e, _) => !matches!(e, Expr::Col(alias, "id" | "a" | "c")
            if self.from.iter().any(|item| {
                item.alias == *alias && matches!(item.source, Source::Table(_))
            })),
            Out::Count(_) => false,
        })
    }
}

impl Expr {
    pub(super) fn sql(&self) -> String {
        match self {
            Self::Col(alias, column) => format!("{alias}.{column}"),
            Self::Lit(Some(value)) => value.to_string(),
            Self::Lit(None) => "NULL".to_owned(),
            Self::Add(left, right) => format!("({} + {})", left.sql(), right.sql()),
            Self::Greatest(left, right) => format!("max({}, {})", left.sql(), right.sql()),
            Self::Sub(query) => format!("({})", query.sql()),
            Self::Coalesce(left, right) => format!("coalesce({}, {})", left.sql(), right.sql()),
        }
    }

    fn json(&self) -> Json {
        match self {
            Self::Col(alias, column) => json!({"col": [alias, column]}),
            Self::Lit(value) => json!({"lit": value}),
            Self::Add(left, right) => json!({"add": [left.json(), right.json()]}),
            Self::Greatest(left, right) => json!({"greatest": [left.json(), right.json()]}),
            Self::Sub(query) => json!({"query": query.json()}),
            Self::Coalesce(left, right) => json!({"coalesce": [left.json(), right.json()]}),
        }
    }

    fn ambiguous(&self) -> bool {
        match self {
            Self::Col(..) | Self::Lit(_) => false,
            Self::Add(left, right) | Self::Greatest(left, right) | Self::Coalesce(left, right) => {
                left.ambiguous() || right.ambiguous()
            }
            Self::Sub(_) => true,
        }
    }
}

impl Cond {
    pub(super) fn sql(&self) -> String {
        match self {
            Self::Cmp(op, left, right) => format!("{} {op} {}", left.sql(), right.sql()),
            Self::Null(e, false) => format!("{} IS NULL", e.sql()),
            Self::Null(e, true) => format!("{} IS NOT NULL", e.sql()),
            Self::InList(e, values) => {
                let values: Vec<String> = values.iter().map(Expr::sql).collect();
                format!("{} IN ({})", e.sql(), values.join(", "))
            }
            Self::InQuery(e, query, not) => {
                format!("{} {}IN ({})", e.sql(), if *not { "NOT " } else { "" }, query.sql())
            }
            Self::Exists(query, not) => {
                format!("{}EXISTS ({})", if *not { "NOT " } else { "" }, query.sql())
            }
            Self::RowSub { values, query, member, first, not } => {
                let values: Vec<String> = values.iter().map(Expr::sql).collect();
                let row = format!("({})", values.join(", "));
                let query = format!("({})", query.sql());
                match (member, not) {
                    (true, not) => format!("{row} {}IN {query}", if *not { "NOT " } else { "" }),
                    (false, not) => {
                        let compared = if *first {
                            format!("{query} = {row}")
                        } else {
                            format!("{row} = {query}")
                        };
                        if *not {
                            format!("NOT ({compared})")
                        } else {
                            compared
                        }
                    }
                }
            }
            Self::RowEq(left, right) => {
                let row = |values: &[Expr]| {
                    let values: Vec<String> = values.iter().map(Expr::sql).collect();
                    format!("({})", values.join(", "))
                };
                format!("{} = {}", row(left), row(right))
            }
            Self::And(left, right) => format!("({} AND {})", left.sql(), right.sql()),
            Self::Or(left, right) => format!("({} OR {})", left.sql(), right.sql()),
            Self::Not(c) => format!("NOT ({})", c.sql()),
        }
    }

    fn json(&self) -> Json {
        match self {
            Self::Cmp(op, left, right) => {
                json!({"cmp": [op.to_ascii_lowercase(), left.json(), right.json()]})
            }
            Self::Null(e, not) => json!({"null": e.json(), "not": not}),
            Self::InList(e, values) => {
                json!({"in": e.json(), "list": values.iter().map(Expr::json).collect::<Vec<_>>()})
            }
            Self::InQuery(e, query, not) => {
                json!({"in": e.json(), "query": query.json(), "not": not})
            }
            Self::Exists(query, not) => json!({"exists": query.json(), "not": not}),
            Self::RowSub { values, query, member, not, .. } => json!({
                "row": values.iter().map(Expr::json).collect::<Vec<_>>(),
                "query": query.json(),
                "member": member,
                "not": not,
            }),
            Self::RowEq(left, right) => json!({"rows": [
                left.iter().map(Expr::json).collect::<Vec<_>>(),
                right.iter().map(Expr::json).collect::<Vec<_>>(),
            ]}),
            Self::And(left, right) => json!({"and": [left.json(), right.json()]}),
            Self::Or(left, right) => json!({"or": [left.json(), right.json()]}),
            Self::Not(c) => json!({"negate": c.json()}),
        }
    }

    fn ambiguous(&self) -> bool {
        match self {
            Self::Cmp(_, left, right) => left.ambiguous() || right.ambiguous(),
            Self::Null(e, _) => e.ambiguous(),
            Self::InList(e, values) => e.ambiguous() || values.iter().any(Expr::ambiguous),
            Self::InQuery(e, query, _) => e.ambiguous() || query.ambiguous(),
            Self::Exists(query, _) => query.ambiguous(),
            // `=` compares with the subquery's first row, whichever SQLite
            // reads first.
            Self::RowSub { values, query, member, .. } => {
                !member || values.iter().any(Expr::ambiguous) || query.ambiguous()
            }
            Self::RowEq(left, right) => left.iter().chain(right).any(Expr::ambiguous),
            Self::And(left, right) | Self::Or(left, right) => left.ambiguous() || right.ambiguous(),
            Self::Not(c) => c.ambiguous(),
        }
    }
}

impl Out {
    fn sql(&self) -> String {
        let (sql, alias) = match self {
            Self::Expr(e, alias) => (e.sql(), alias),
            Self::Count(alias) => ("count(*)".to_owned(), alias),
            Self::Max(e, alias) => (format!("max({})", e.sql()), alias),
            Self::Concat(e, order, alias) => {
                let order = order_sql(order).map_or_else(String::new, |order| format!(" {order}"));
                (format!("CAST(group_concat({}, ''{order}) AS INTEGER)", e.sql()), alias)
            }
            Self::RowNumber(partition, order, alias) => {
                let mut window = Vec::new();
                if !partition.is_empty() {
                    let terms: Vec<String> = partition.iter().map(Expr::sql).collect();
                    window.push(format!("PARTITION BY {}", terms.join(", ")));
                }
                window.extend(order_sql(order));
                (format!("row_number() OVER ({})", window.join(" ")), alias)
            }
        };
        match alias {
            Some(alias) => format!("{sql} AS {alias}"),
            None => sql,
        }
    }

    fn json(&self) -> Json {
        match self {
            Self::Expr(e, alias) => json!({"expr": e.json(), "as": alias}),
            Self::Count(alias) => json!({"count": true, "as": alias}),
            Self::Max(e, alias) => json!({"max": e.json(), "as": alias}),
            Self::Concat(e, order, alias) => json!({
                "concat": e.json(),
                "order": order_json(order),
                "as": alias,
            }),
            Self::RowNumber(partition, order, alias) => json!({
                "row_number": {
                    "partition": partition.iter().map(Expr::json).collect::<Vec<_>>(),
                    "order": order_json(order),
                },
                "as": alias,
            }),
        }
    }
}

/// A call's or a window's `ORDER BY`, each term descending when `true`, or
/// `None` for no terms.
fn order_sql(order: &[(Expr, bool)]) -> Option<String> {
    let terms: Vec<String> = order
        .iter()
        .map(|(sorted, descending)| {
            format!("{}{}", sorted.sql(), if *descending { " DESC" } else { "" })
        })
        .collect();
    (!terms.is_empty()).then(|| format!("ORDER BY {}", terms.join(", ")))
}

fn order_json(order: &[(Expr, bool)]) -> Vec<Json> {
    order
        .iter()
        .map(|(sorted, descending)| json!({"expr": sorted.json(), "desc": descending}))
        .collect()
}
