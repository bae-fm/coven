//! The parsed SELECT a scope plan reads: its parameters numbered in source
//! order, and the expressions inside it (§4).

use rusqlite::{Connection, Statement};
use std::collections::BTreeMap;

use sqlite3_parser::ast::{
    As, Expr, FrameBound, FunctionCallOrder, FunctionTail, Literal, Name, Operator, Over,
    ResultColumn, Select, SortedColumn, Window,
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
    /// A function called by name, with how many arguments, and whether it
    /// runs over a window.
    Function(&'a str, usize, bool),
    /// `CURRENT_DATE`, `CURRENT_TIME` or `CURRENT_TIMESTAMP`, which read
    /// the clock.
    Clock,
}

impl Part<'_> {
    /// Whether it calls a function over a window.
    pub(crate) fn windowed(&self) -> bool {
        matches!(self, Part::Function(_, _, true))
    }

    /// Whether it calls an aggregate of the connection's `functions`,
    /// outside any window.
    pub(crate) fn aggregates(&self, functions: &Functions) -> bool {
        matches!(self, Part::Function(name, arguments, false) if functions.aggregate(name, *arguments))
    }

    /// Whether it makes one row of many: it calls an aggregate, or a
    /// function over a window.
    pub(crate) fn many_rows(&self, functions: &Functions) -> bool {
        self.windowed() || self.aggregates(functions)
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
    PartWalk { inside: &|_| false, parts: into, stopped: &mut Vec::new() }.expr(expr);
}

/// The parts of `expr` outside the expressions `inside` picks, which go to
/// `stopped` whole, without their parts.
pub(crate) fn parts_outside<'a>(
    expr: &'a Expr<'a>,
    inside: &dyn Fn(&'a Expr<'a>) -> bool,
    into: &mut Vec<Part<'a>>,
    stopped: &mut Vec<&'a Expr<'a>>,
) {
    PartWalk { inside, parts: into, stopped }.expr(expr);
}

/// The parts of a window's partitions, order and frame.
pub(crate) fn window_parts<'a>(window: &'a Window<'a>, into: &mut Vec<Part<'a>>) {
    PartWalk { inside: &|_| false, parts: into, stopped: &mut Vec::new() }.window(window);
}

/// A walk of an expression's parts that stops at the expressions `inside`
/// picks.
struct PartWalk<'a, 'w> {
    inside: &'w dyn Fn(&'a Expr<'a>) -> bool,
    parts: &'w mut Vec<Part<'a>>,
    stopped: &'w mut Vec<&'a Expr<'a>>,
}

impl<'a> PartWalk<'a, '_> {
    fn expr(&mut self, expr: &'a Expr<'a>) {
        if (self.inside)(expr) {
            self.stopped.push(expr);
            return;
        }
        match expr {
            Expr::Id(_) | Expr::Name(_) | Expr::Qualified(..) | Expr::DoublyQualified(..) => {
                self.parts.push(Part::Name(expr))
            }
            Expr::Exists(select) => self.parts.push(Part::Select(select)),
            Expr::Subquery(select) => self.parts.push(Part::Subquery(select)),
            Expr::InSelect { lhs, rhs, .. } => {
                self.expr(lhs);
                self.parts.push(Part::Select(rhs));
            }
            Expr::InTable { lhs, args, .. } => {
                self.expr(lhs);
                self.parts.push(Part::Table);
                self.every(args.iter().flat_map(|args| args.iter()));
            }
            Expr::Between { lhs, start, end, .. } => self.every([*lhs, *start, *end]),
            Expr::Binary(left, _, right) => self.every([*left, *right]),
            Expr::Case { base, when_then_pairs, else_expr } => {
                self.every(base.iter().copied());
                for (when, then) in when_then_pairs.iter() {
                    self.every([when, then]);
                }
                self.every(else_expr.iter().copied());
            }
            Expr::Collate(expr, _) => {
                self.parts.push(Part::Collation);
                self.expr(expr);
            }
            Expr::Cast { expr, .. }
            | Expr::IsNull(expr)
            | Expr::NotNull(expr)
            | Expr::Unary(_, expr) => self.expr(expr),
            Expr::FunctionCall { name, args, order_by, filter_over, .. } => {
                let arguments = args.as_ref().map_or(0, |args| args.len());
                self.parts.push(Part::Function(name.0, arguments, over(filter_over)));
                self.every(args.iter().flat_map(|args| args.iter()));
                self.every(call_order(order_by).iter().map(|term| &term.expr));
                if let Some(tail) = filter_over {
                    self.tail(tail);
                }
            }
            Expr::FunctionCallStar { name, filter_over } => {
                self.parts.push(Part::Function(name.0, 0, over(filter_over)));
                if let Some(tail) = filter_over {
                    self.tail(tail);
                }
            }
            Expr::InList { lhs, rhs, .. } => {
                self.expr(lhs);
                self.every(rhs.iter().flat_map(|exprs| exprs.iter()));
            }
            Expr::Like { lhs, rhs, escape, .. } => {
                self.every([*lhs, *rhs]);
                self.every(escape.iter().copied());
            }
            Expr::Parenthesized(exprs) => self.every(exprs.iter()),
            Expr::Raise(_, expr) => self.every(expr.iter().copied()),
            Expr::Literal(
                Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp,
            ) => self.parts.push(Part::Clock),
            Expr::Literal(_) | Expr::Variable(_) => {}
        }
    }

    fn every(&mut self, exprs: impl IntoIterator<Item = &'a Expr<'a>>) {
        for expr in exprs {
            self.expr(expr);
        }
    }

    fn tail(&mut self, tail: &'a FunctionTail<'a>) {
        self.every(tail.filter_clause.iter().copied());
        if let Some(Over::Window(window)) = tail.over_clause {
            self.window(window);
        }
    }

    fn window(&mut self, window: &'a Window<'a>) {
        self.every(window.partition_by.iter().flat_map(|exprs| exprs.iter()));
        self.every(window.order_by.iter().flat_map(|terms| terms.iter()).map(|term| &term.expr));
        if let Some(frame) = &window.frame_clause {
            for bound in [Some(&frame.start), frame.end.as_ref()].into_iter().flatten() {
                if let FrameBound::Following(expr) | FrameBound::Preceding(expr) = bound {
                    self.expr(expr);
                }
            }
        }
    }
}

fn over(tail: &Option<FunctionTail<'_>>) -> bool {
    tail.as_ref().is_some_and(|tail| tail.over_clause.is_some())
}

/// The terms of a call's own ORDER BY.
fn call_order<'a>(order_by: &Option<FunctionCallOrder<'a>>) -> &'a [SortedColumn<'a>] {
    match order_by {
        Some(FunctionCallOrder::SortList(terms)) => terms,
        None => &[],
    }
}

/// A call of one of the connection's aggregate functions as an aggregate,
/// outside any window: its name, lowercase, and the terms of its own ORDER
/// BY.
pub(crate) struct AggregateCall<'a> {
    pub(crate) name: String,
    pub(crate) order: &'a [SortedColumn<'a>],
}

/// The aggregate call `expr` is, when it calls an aggregate of the
/// connection's `functions` outside any window, as `Part::aggregates` reads a
/// call.
pub(crate) fn aggregate_call<'a>(
    expr: &'a Expr<'a>,
    functions: &Functions,
) -> Option<AggregateCall<'a>> {
    let (name, arguments, order, tail) = match expr {
        Expr::FunctionCall { name, args, order_by, filter_over, .. } => {
            (name.0, args.as_ref().map_or(0, |args| args.len()), call_order(order_by), filter_over)
        }
        Expr::FunctionCallStar { name, filter_over } => (name.0, 0, &[][..], filter_over),
        _ => return None,
    };
    let name = name.to_ascii_lowercase();
    (!over(tail) && functions.aggregate(&name, arguments)).then_some(AggregateCall { name, order })
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

/// One of the definitions SQLite keeps for a function's name.
struct Definition {
    /// How many arguments it takes, as `pragma_function_list` reports it: a
    /// count, -1 for any number, -3 for one or more, -4 for two or more.
    arguments: i64,
    /// An aggregate, which a window function is too outside a window.
    aggregate: bool,
    /// A scalar function the connection flags deterministic.
    deterministic: bool,
}

impl Definition {
    fn takes(&self, arguments: usize) -> bool {
        let arguments = i64::try_from(arguments).unwrap_or(i64::MAX);
        match self.arguments {
            -1 => true,
            least @ ..=-3 => arguments >= -2 - least,
            count => count == arguments,
        }
    }
}

/// The connection's functions, as SQLite reads a call: by its name and how
/// many arguments it passes. A definition that takes exactly that many
/// arguments wins over one that takes any number, so `max(a)` calls the
/// aggregate and `max(a, b)` the scalar function.
pub(crate) struct Functions(BTreeMap<String, Vec<Definition>>);

impl Functions {
    pub(crate) fn read(connection: &Connection) -> rusqlite::Result<Self> {
        let mut listed = connection
            .prepare_cached("SELECT name, narg, type, flags FROM pragma_function_list")?;
        let mut functions: BTreeMap<String, Vec<Definition>> = BTreeMap::new();
        let rows = listed.query_map([], |row| {
            let kind: String = row.get(2)?;
            let flags: i64 = row.get(3)?;
            Ok((
                row.get::<_, String>(0)?.to_ascii_lowercase(),
                Definition {
                    arguments: row.get(1)?,
                    aggregate: kind != "s",
                    deterministic: kind == "s" && flags & DETERMINISTIC != 0,
                },
            ))
        })?;
        for row in rows {
            let (name, definition) = row?;
            functions.entry(name).or_default().push(definition);
        }
        Ok(Self(functions))
    }

    /// The definitions a call of `name` with `arguments` arguments can run:
    /// those that take exactly that many, or else those that take a number
    /// of arguments that includes it.
    fn called(&self, name: &str, arguments: usize) -> Vec<&Definition> {
        let Some(definitions) = self.0.get(&name.to_ascii_lowercase()) else { return Vec::new() };
        let exact: Vec<&Definition> = definitions
            .iter()
            .filter(|definition| definition.arguments >= 0 && definition.takes(arguments))
            .collect();
        if !exact.is_empty() {
            return exact;
        }
        definitions.iter().filter(|definition| definition.takes(arguments)).collect()
    }

    /// Whether a call of `name` with `arguments` arguments, outside any
    /// window, calls an aggregate.
    pub(crate) fn aggregate(&self, name: &str, arguments: usize) -> bool {
        self.called(name, arguments).iter().any(|definition| definition.aggregate)
    }

    /// Whether a call of `name` with `arguments` arguments calls a scalar
    /// function the connection flags deterministic.
    pub(crate) fn deterministic(&self, name: &str, arguments: usize) -> bool {
        let called = self.called(name, arguments);
        !called.is_empty() && called.iter().all(|definition| definition.deterministic)
    }

    /// Whether evaluating the expression `found` holds twice gives the same
    /// answer: it names no clock and every function in it is deterministic.
    pub(crate) fn repeatable(&self, found: &[Part<'_>]) -> bool {
        found.iter().all(|part| match part {
            Part::Clock => false,
            Part::Function(name, arguments, _) => {
                !is_clock_function(&name.to_ascii_lowercase())
                    && self.deterministic(name, *arguments)
            }
            _ => true,
        })
    }
}
