//! The parsed SELECT a scope plan reads: its parameters numbered in source
//! order, and the expressions inside it (§4).

use rusqlite::{Connection, Statement};
use std::collections::BTreeSet;

use sqlite3_parser::ast::{
    As, Expr, FrameBound, FunctionCallOrder, FunctionTail, Literal, Name, Operator, Over,
    ResultColumn, Select, Window,
};
use sqlite3_parser::lexer::{
    sql::{TokenType, Tokenizer},
    Scanner,
};

// Number every parameter in source order, before interpreting any predicate.
// The parser's AST normalizes LIMIT offset,count into count/OFFSET, so use its
// lexer to preserve source order. Only variables are rewritten; conditions are
// recognized from the parsed SELECT and expression nodes, never token matching.
pub(crate) fn number_parameters(
    sql: &str,
    statement: &Statement<'_>,
) -> rusqlite::Result<Option<String>> {
    let mut scanner = Scanner::new(Tokenizer::new());
    let mut next = 1;
    let mut end_of_previous = 0;
    let mut numbered = String::new();
    loop {
        let Ok((start, token, end)) = scanner.scan(sql.as_bytes()) else {
            return Ok(None);
        };
        let Some((_, kind)) = token else { break };
        if kind != TokenType::TK_VARIABLE {
            continue;
        }
        let name = &sql[start..end];
        let index = if name == "?" {
            next
        } else if let Some(digits) = name.strip_prefix('?') {
            digits.parse::<usize>().expect("SQLite accepted this parameter index")
        } else {
            statement
                .parameter_index(name)?
                .expect("parsed parameter belongs to prepared statement")
        };
        next = next.max(index + 1);
        numbered.push_str(&sql[end_of_previous..start]);
        numbered.push_str(&format!("?{index}"));
        end_of_previous = end;
    }
    numbered.push_str(&sql[end_of_previous..]);
    Ok(Some(numbered))
}

/// The conditions `expr` joins with AND, each once.
pub(crate) fn conjuncts<'a>(expr: &'a Expr<'a>, into: &mut Vec<&'a Expr<'a>>) {
    match crate::sql::unparenthesized(expr) {
        Expr::Binary(left, Operator::And, right) => {
            conjuncts(left, into);
            conjuncts(right, into);
        }
        expr => into.push(expr),
    }
}

/// What an expression holds, outside the SELECTs nested in it.
pub(crate) enum Part<'a> {
    /// A name SQLite could resolve to a column: bare, qualified by a table,
    /// or by a schema and a table.
    Name(&'a Expr<'a>),
    /// A SELECT, with sources of its own, that an EXISTS or an IN reads.
    Select(&'a Select<'a>),
    /// A SELECT whose first row the expression takes: a scalar subquery, or
    /// the subquery a row value is compared with.
    Subquery(&'a Select<'a>),
    /// `x IN table`: a table read outside any FROM.
    Table,
    /// A COLLATE, which gives a comparison holding it its collation.
    Collation,
    /// A function called by name, and whether it runs over a window.
    Function(&'a str, bool),
    /// `CURRENT_DATE`, `CURRENT_TIME` or `CURRENT_TIMESTAMP`, which read
    /// the clock.
    Clock,
}

impl Part<'_> {
    /// Whether it calls a function over a window.
    pub(crate) fn windowed(&self) -> bool {
        matches!(self, Part::Function(_, true))
    }

    /// Whether it calls one of `aggregates`, the connection's aggregate and
    /// window functions, lowercase, as an aggregate outside any window.
    pub(crate) fn aggregates(&self, aggregates: &BTreeSet<String>) -> bool {
        matches!(self, Part::Function(name, false) if aggregates.contains(&name.to_ascii_lowercase()))
    }

    /// Whether it makes one row of many: it calls an aggregate, or a
    /// function over a window.
    pub(crate) fn many_rows(&self, aggregates: &BTreeSet<String>) -> bool {
        self.windowed() || self.aggregates(aggregates)
    }
}

/// What an ORDER BY term sorts by, as SQLite reads it against the result
/// columns `columns`.
pub(crate) enum OrderTerm<'a> {
    /// The expression of the result column whose position the term gives
    /// as its number.
    Position(&'a Expr<'a>),
    /// The result column at a position, from zero, whose alias the term, a
    /// bare name, matches, which SQLite reads before any other column of
    /// that name, and the column's expression.
    Alias(usize, &'a Expr<'a>),
    /// An expression of its own, which SQLite resolves among the FROM items.
    Expr(&'a Expr<'a>),
    /// A result column sqlive can't place: one after a `*`, whose position
    /// sqlive doesn't know as it doesn't know how many columns a `*` adds, a
    /// `*` itself, a number past the last column, or an alias several result
    /// columns share.
    Unknown,
}

/// What the ORDER BY term `expr` sorts by, among the result columns
/// `columns`.
pub(crate) fn order_term<'a>(columns: &'a [ResultColumn<'a>], expr: &'a Expr<'a>) -> OrderTerm<'a> {
    let result_column = |position: usize| match columns.get(position) {
        Some(ResultColumn::Expr(expr, _))
            if columns[..position].iter().all(|column| {
                !matches!(column, ResultColumn::Star | ResultColumn::TableStar(_))
            }) =>
        {
            Some(expr)
        }
        _ => None,
    };
    let expr = crate::sql::unparenthesized(expr);
    let bare = match expr {
        Expr::Literal(Literal::Numeric(number)) => {
            let position = number.parse::<usize>().ok().and_then(|number| number.checked_sub(1));
            return match position.and_then(result_column) {
                Some(column) => OrderTerm::Position(column),
                None => OrderTerm::Unknown,
            };
        }
        Expr::Id(name) => Name(name.0),
        Expr::Name(name) => Name(name.0),
        _ => return OrderTerm::Expr(expr),
    };
    let named: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            matches!(column, ResultColumn::Expr(_, Some(As::As(alias) | As::Elided(alias)))
                if Name(alias.0) == bare)
        })
        .map(|(position, _)| position)
        .collect();
    match named[..] {
        [] => OrderTerm::Expr(expr),
        [position] => match result_column(position) {
            Some(column) => OrderTerm::Alias(position, column),
            None => OrderTerm::Unknown,
        },
        _ => OrderTerm::Unknown,
    }
}

/// The parts of `expr`, in every operand, argument, filter and window.
pub(crate) fn parts<'a>(expr: &'a Expr<'a>, into: &mut Vec<Part<'a>>) {
    match expr {
        Expr::Id(_) | Expr::Name(_) | Expr::Qualified(..) | Expr::DoublyQualified(..) => {
            into.push(Part::Name(expr))
        }
        Expr::Exists(select) => into.push(Part::Select(select)),
        Expr::Subquery(select) => into.push(Part::Subquery(select)),
        Expr::InSelect { lhs, rhs, .. } => {
            parts(lhs, into);
            into.push(Part::Select(rhs));
        }
        Expr::InTable { lhs, args, .. } => {
            parts(lhs, into);
            into.push(Part::Table);
            every(args.iter().flat_map(|args| args.iter()), into);
        }
        Expr::Between { lhs, start, end, .. } => every([*lhs, *start, *end], into),
        Expr::Binary(left, _, right) => every([*left, *right], into),
        Expr::Case { base, when_then_pairs, else_expr } => {
            every(base.iter().copied(), into);
            for (when, then) in when_then_pairs.iter() {
                every([when, then], into);
            }
            every(else_expr.iter().copied(), into);
        }
        Expr::Collate(expr, _) => {
            into.push(Part::Collation);
            parts(expr, into);
        }
        Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => parts(expr, into),
        Expr::FunctionCall { name, args, order_by, filter_over, .. } => {
            into.push(Part::Function(name.0, over(filter_over)));
            every(args.iter().flat_map(|args| args.iter()), into);
            match order_by {
                Some(FunctionCallOrder::SortList(terms)) => {
                    every(terms.iter().map(|term| &term.expr), into)
                }
                None => {}
            }
            if let Some(tail) = filter_over {
                tail_parts(tail, into);
            }
        }
        Expr::FunctionCallStar { name, filter_over } => {
            into.push(Part::Function(name.0, over(filter_over)));
            if let Some(tail) = filter_over {
                tail_parts(tail, into);
            }
        }
        Expr::InList { lhs, rhs, .. } => {
            parts(lhs, into);
            every(rhs.iter().flat_map(|exprs| exprs.iter()), into);
        }
        Expr::Like { lhs, rhs, escape, .. } => {
            every([*lhs, *rhs], into);
            every(escape.iter().copied(), into);
        }
        Expr::Parenthesized(exprs) => every(exprs.iter(), into),
        Expr::Raise(_, expr) => every(expr.iter().copied(), into),
        Expr::Literal(Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp) => {
            into.push(Part::Clock)
        }
        Expr::Literal(_) | Expr::Variable(_) => {}
    }
}

fn over(tail: &Option<FunctionTail<'_>>) -> bool {
    tail.as_ref().is_some_and(|tail| tail.over_clause.is_some())
}

fn every<'a>(exprs: impl IntoIterator<Item = &'a Expr<'a>>, into: &mut Vec<Part<'a>>) {
    for expr in exprs {
        parts(expr, into);
    }
}

fn tail_parts<'a>(tail: &'a FunctionTail<'a>, into: &mut Vec<Part<'a>>) {
    every(tail.filter_clause.iter().copied(), into);
    if let Some(Over::Window(window)) = tail.over_clause {
        window_parts(window, into);
    }
}

/// The parts of a window's partitions, order and frame.
pub(crate) fn window_parts<'a>(window: &'a Window<'a>, into: &mut Vec<Part<'a>>) {
    every(window.partition_by.iter().flat_map(|exprs| exprs.iter()), into);
    every(window.order_by.iter().flat_map(|terms| terms.iter()).map(|term| &term.expr), into);
    if let Some(frame) = &window.frame_clause {
        for bound in [Some(&frame.start), frame.end.as_ref()].into_iter().flatten() {
            if let FrameBound::Following(expr) | FrameBound::Preceding(expr) = bound {
                parts(expr, into);
            }
        }
    }
}

/// A parsed node written back as SQL.
pub(crate) fn text(node: &impl sqlite3_parser::ast::fmt::ToTokens) -> String {
    struct Text<'a, T>(&'a T);
    impl<T: sqlite3_parser::ast::fmt::ToTokens> std::fmt::Display for Text<'_, T> {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.to_fmt(formatter)
        }
    }
    Text(node).to_string()
}

/// SQLite's date and time functions, which it flags deterministic but which
/// read the clock for `'now'`.
const CLOCK_FUNCTIONS: [&str; 7] =
    ["date", "time", "datetime", "julianday", "unixepoch", "strftime", "timediff"];

/// SQLITE_DETERMINISTIC, as `pragma_function_list` reports it in `flags`.
const DETERMINISTIC: i64 = 0x800;

/// Whether `name` is one of SQLite's date and time functions, which may read
/// the clock.
pub(crate) fn is_clock_function(name: &str) -> bool {
    CLOCK_FUNCTIONS.contains(&name)
}

/// Whether every function of `names` is a scalar function the connection
/// flags deterministic, at every arity it has.
pub(crate) fn deterministic(connection: &Connection, names: &[String]) -> rusqlite::Result<bool> {
    let mut flags = connection.prepare_cached(
        "SELECT count(*), min(flags & ?2 = ?2 AND type = 's') \
         FROM pragma_function_list WHERE name = ?1 COLLATE NOCASE",
    )?;
    for name in names {
        let (found, all): (i64, Option<i64>) = flags
            .query_row(rusqlite::params![name, DETERMINISTIC], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
        if found == 0 || all != Some(1) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether evaluating the expression `found` holds twice gives the same
/// answer: it names no clock and every function in it is deterministic.
pub(crate) fn repeatable(connection: &Connection, found: &[Part<'_>]) -> rusqlite::Result<bool> {
    let mut functions = Vec::new();
    for part in found {
        match part {
            Part::Clock => return Ok(false),
            Part::Function(name, _) => {
                let name = name.to_ascii_lowercase();
                if is_clock_function(&name) {
                    return Ok(false);
                }
                functions.push(name);
            }
            _ => {}
        }
    }
    deterministic(connection, &functions)
}
