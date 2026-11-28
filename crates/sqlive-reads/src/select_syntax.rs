//! The parsed SELECT a scope plan reads: its parameters numbered in source
//! order, and the expressions inside it (SPEC.md section 4).

use rusqlite::{Connection, Statement};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use sqlite3_parser::ast::{
    As, Cmd, Expr, FrameBound, FromClause, FunctionCallOrder, FunctionTail, LikeOperator, Literal,
    Name, Operator, Over, QualifiedName, ResultColumn, Select, SelectTable, SortedColumn, Stmt,
    Window,
};
use sqlite3_parser::lexer::sql::{Error as ParseError, Parser, TokenType};
use sqlite3_parser::{Bump, FallibleIterator};

// Number every parameter in source order, before interpreting any predicate.
// The parser's AST normalizes LIMIT offset,count into count/OFFSET, so read the
// tokens to preserve source order. Only variables are rewritten; conditions are
// recognized from the parsed SELECT and expression nodes, never token matching.
pub(crate) fn number_parameters(sql: &str, statement: &Statement<'_>) -> rusqlite::Result<String> {
    let mut next = 1;
    let mut end_of_previous = 0;
    let mut numbered = String::new();
    for (kind, range) in crate::token_scan::scan(sql)? {
        if kind != TokenType::TK_VARIABLE {
            continue;
        }
        let name = &sql[range.clone()];
        let index = if name == "?" {
            next
        } else if let Some(digits) = name.strip_prefix('?') {
            digits.parse::<usize>().expect("SQLite accepted this parameter index")
        } else {
            statement
                .parameter_index(name)?
                .expect("scanned parameter belongs to prepared statement")
        };
        next = next.max(index + 1);
        numbered.push_str(&sql[end_of_previous..range.start]);
        numbered.push_str(&format!("?{index}"));
        end_of_previous = range.end;
    }
    numbered.push_str(&sql[end_of_previous..]);
    Ok(numbered)
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
    /// `x IN table`: a table, view or CTE read outside any FROM, by its
    /// name.
    Table(&'a QualifiedName<'a>),
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

    /// Whether it is a call that gives the answer a lookup that evaluates it
    /// again at a write gets: an aggregate or a window function takes its
    /// rows' answer, and any other function must be deterministic. False for
    /// a part that is no call.
    pub(crate) fn answers_again(&self, functions: &Functions) -> bool {
        match self {
            Part::Function(name, arguments, _) => {
                self.many_rows(functions) || functions.repeats(name, *arguments)
            }
            _ => false,
        }
    }
}

/// Why the parser reads no one SELECT in some SQL: SQLite and the parser
/// can accept different language versions.
#[derive(Debug)]
pub(crate) enum NotOneSelect {
    /// It reads another statement, none, or more than one.
    Statements,
    /// It can't read the SQL.
    Unreadable(ParseError),
}

impl std::fmt::Display for NotOneSelect {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Statements => {
                formatter.write_str("the statement isn't one SELECT the parser reads")
            }
            Self::Unreadable(error) => {
                write!(formatter, "the parser can't read the statement: {error}")
            }
        }
    }
}

/// The SELECT that `sql` is, read into `bump`.
pub(crate) fn one_select<'b>(bump: &'b Bump, sql: &'b str) -> Result<&'b Select<'b>, NotOneSelect> {
    let mut parser = Parser::new(bump, sql.as_bytes());
    let select = match parser.next() {
        Ok(Some(Cmd::Stmt(Stmt::Select(select)))) => select,
        Ok(_) => return Err(NotOneSelect::Statements),
        Err(error) => return Err(NotOneSelect::Unreadable(error)),
    };
    match parser.next() {
        Ok(None) => Ok(select),
        Ok(Some(_)) => Err(NotOneSelect::Statements),
        Err(error) => Err(NotOneSelect::Unreadable(error)),
    }
}

/// What an ORDER BY term sorts by, as SQLite reads it against the result
/// columns `columns`.
pub(crate) enum OrderTerm<'a> {
    /// The expression of the result column whose position the term gives
    /// as its number.
    Position(&'a Expr<'a>),
    /// The expression of the result column whose alias the term, a bare
    /// name, matches, which SQLite reads before any other column of that
    /// name.
    Alias(&'a Expr<'a>),
    /// An expression of its own, which SQLite resolves among the FROM items.
    Expr(&'a Expr<'a>),
    /// A result column sqlive can't place: a number after a `*`, since this
    /// doesn't count the columns a `*` adds, a `*` itself, a number past the
    /// last column, or an alias several result columns share.
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
        [position] => match &columns[position] {
            ResultColumn::Expr(column, _) => OrderTerm::Alias(column),
            ResultColumn::Star | ResultColumn::TableStar(_) => {
                unreachable!("a `*` has no alias")
            }
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
            Expr::InTable { lhs, rhs, args, .. } => {
                self.expr(lhs);
                self.parts.push(Part::Table(rhs));
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

/// Whether `expr` is NULL wherever a column it names is NULL: every
/// operation in it gives NULL where one of its operands is NULL. Columns,
/// constants and parameters are, and so are arithmetic, comparisons, `||`,
/// the bit operators, `NOT`, the signs, CAST, COLLATE and a call of one of
/// SQLite's built-in functions that `Functions::strict` names, with
/// `LIKE`, `GLOB`, `->` and `->>`, which call `like`, `glob`, `->` and `->>`.
/// The rest can give a value where every column they name is NULL, as a
/// LEFT JOIN's missing row gives: CASE, `IS`, `IS NOT`, `IS NULL`, `NOT
/// NULL`, AND, OR (`NULL OR 1` is 1), BETWEEN (`1 BETWEEN c AND 0` is 0), an
/// IN list (`1 IN (c, 1)` is 1), `REGEXP` and `MATCH`, which call the app's
/// functions, a subquery, and any other function, as `coalesce`, `iif`,
/// `nullif`, `quote`, `typeof`, `concat` or an app's own.
pub(crate) fn strict(expr: &Expr<'_>, functions: &Functions) -> bool {
    match expr {
        Expr::Id(_)
        | Expr::Name(_)
        | Expr::Qualified(..)
        | Expr::DoublyQualified(..)
        | Expr::Literal(_)
        | Expr::Variable(_) => true,
        Expr::Binary(left, operator, right) => {
            let called = match operator {
                Operator::And | Operator::Or | Operator::Is | Operator::IsNot => false,
                Operator::ArrowRight => functions.strict("->", 2),
                Operator::ArrowRightShift => functions.strict("->>", 2),
                Operator::Add
                | Operator::Subtract
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulus
                | Operator::Concat
                | Operator::BitwiseAnd
                | Operator::BitwiseOr
                | Operator::LeftShift
                | Operator::RightShift
                | Operator::Equals
                | Operator::NotEquals
                | Operator::Less
                | Operator::LessEquals
                | Operator::Greater
                | Operator::GreaterEquals => true,
            };
            called && strict(left, functions) && strict(right, functions)
        }
        Expr::Unary(_, expr) | Expr::Cast { expr, .. } | Expr::Collate(expr, _) => {
            strict(expr, functions)
        }
        Expr::Parenthesized(exprs) => matches!(&exprs[..], [expr] if strict(expr, functions)),
        Expr::Like { lhs, op, rhs, escape, .. } => {
            let name = match op {
                LikeOperator::Like => "like",
                LikeOperator::Glob => "glob",
                LikeOperator::Match => "match",
                LikeOperator::Regexp => "regexp",
            };
            functions.strict(name, 2 + usize::from(escape.is_some()))
                && [*lhs, *rhs].into_iter().chain(*escape).all(|expr| strict(expr, functions))
        }
        Expr::FunctionCall {
            name,
            distinctness: None,
            args,
            order_by: None,
            filter_over: None,
        } => {
            let args = args.unwrap_or_default();
            functions.strict(name.0, args.len()) && args.iter().all(|arg| strict(arg, functions))
        }
        Expr::FunctionCall { .. }
        | Expr::FunctionCallStar { .. }
        | Expr::Between { .. }
        | Expr::Case { .. }
        | Expr::InList { .. }
        | Expr::InSelect { .. }
        | Expr::InTable { .. }
        | Expr::Exists(_)
        | Expr::Subquery(_)
        | Expr::IsNull(_)
        | Expr::NotNull(_)
        | Expr::Raise(..) => false,
    }
}

/// The OVER clause of the window function `expr` calls, when it calls one.
pub(crate) fn window_call<'a>(expr: &'a Expr<'a>) -> Option<&'a Over<'a>> {
    let (Expr::FunctionCall { filter_over, .. } | Expr::FunctionCallStar { filter_over, .. }) =
        expr
    else {
        return None;
    };
    filter_over.as_ref()?.over_clause
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
/// outside any window: its name, lowercase, its arguments and the terms of
/// its own ORDER BY.
pub(crate) struct AggregateCall<'a> {
    pub(crate) name: String,
    pub(crate) arguments: &'a [Expr<'a>],
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
            (name.0, args.unwrap_or_default(), call_order(order_by), filter_over)
        }
        Expr::FunctionCallStar { name, filter_over } => (name.0, &[][..], &[][..], filter_over),
        _ => return None,
    };
    let name = name.to_ascii_lowercase();
    (!over(tail) && functions.aggregate(&name, arguments.len())).then_some(AggregateCall {
        name,
        arguments,
        order,
    })
}

/// The expressions of the result columns `results`.
pub(crate) fn result_expressions<'a>(
    results: &'a [ResultColumn<'a>],
) -> impl Iterator<Item = &'a Expr<'a>> {
    results.iter().filter_map(|result| match result {
        ResultColumn::Expr(expr, _) => Some(expr),
        ResultColumn::Star | ResultColumn::TableStar(_) => None,
    })
}

/// Which core SQLite gives each aggregate call of the result columns
/// `results` of a core whose FROM is `from`, outside their subqueries, where
/// `between` holds the names of the FROMs between that core and one around it:
/// the core's own are added to them. A call goes past all those FROMs when it
/// qualifies columns, none with one of their names, and then has the names
/// that qualify its columns. Else it stays among them, and has `None`. SQL
/// that passes an aggregate an outer column writes the column's qualifier
/// (sqlive's SPEC.md, section 12). Adds to `found` each part of the result
/// columns, those of the calls too.
pub(crate) fn aggregate_owners<'a>(
    results: &'a [ResultColumn<'a>],
    from: Option<&'a FromClause<'a>>,
    between: &mut Vec<&'a str>,
    functions: &Functions,
    found: &mut Vec<Part<'a>>,
) -> Vec<Option<Vec<&'a Name<'a>>>> {
    if let Some(from) = from {
        from_names(from, between);
    }
    let mut calls = Vec::new();
    for expr in result_expressions(results) {
        let aggregate = |expr| aggregate_call(expr, functions).is_some();
        parts_outside(expr, &aggregate, found, &mut calls);
    }
    calls
        .into_iter()
        .map(|call| {
            let first = found.len();
            parts(call, found);
            let qualifiers: Vec<_> = found[first..]
                .iter()
                .filter_map(|part| match part {
                    Part::Name(name) => match crate::sql::unparenthesized(name) {
                        Expr::Qualified(table, _) | Expr::DoublyQualified(_, table, _) => {
                            Some(table)
                        }
                        _ => None,
                    },
                    _ => None,
                })
                .collect();
            let past = !qualifiers.is_empty()
                && qualifiers
                    .iter()
                    .all(|&qualifier| !between.iter().any(|name| *qualifier == Name(name)));
            past.then_some(qualifiers)
        })
        .collect()
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

/// A parsed node written back as SQL, with each name whose text `renamed`
/// holds, by the address of that text, written as the SQL it gives instead.
pub(crate) fn text_renamed(
    node: &impl sqlite3_parser::ast::fmt::ToTokens,
    renamed: &[(usize, String)],
) -> String {
    use sqlite3_parser::dialect::TokenType;
    struct Renaming<'a> {
        written: String,
        renamed: &'a [(usize, String)],
    }
    impl sqlite3_parser::ast::fmt::TokenStream for Renaming<'_> {
        type Error = std::convert::Infallible;

        fn append(&mut self, ty: TokenType, value: Option<&str>) -> Result<(), Self::Error> {
            let value = value.map(|value| {
                let address = value.as_ptr() as usize;
                match self.renamed.iter().find(|(at, _)| *at == address) {
                    Some((_, sql)) => sql.as_str(),
                    None => value,
                }
            });
            // Each token goes as SQLite's own printer writes it, apart from
            // its neighbours by a space, so that no two run together.
            if !self.written.is_empty() {
                self.written.push(' ');
            }
            if ty == TokenType::TK_BLOB {
                self.written.push_str("X'");
                self.written.push_str(value.unwrap_or_default());
                self.written.push('\'');
                return Ok(());
            }
            if let Some(keyword) = ty.as_str() {
                self.written.push_str(keyword);
            }
            if let Some(value) = value {
                self.written.push_str(value);
            }
            Ok(())
        }
    }
    let mut stream = Renaming { written: String::new(), renamed };
    match node.to_tokens(&mut stream) {
        Ok(()) => stream.written,
        Err(never) => match never {},
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

/// SQLite's built-in scalar functions that return NULL whenever one of their
/// arguments is NULL, as a call of each with a NULL in each place shows. The
/// date and time functions do too, but read the clock. Every other built-in
/// one can return a value for a NULL argument: `coalesce(NULL, 1)`,
/// `ifnull`, `iif` and `if` pick another argument; `nullif(1, NULL)` is 1;
/// `quote`, `typeof`, `hex`, `char`, `zeroblob`, `subtype`, `soundex` and
/// `unistr_quote` read NULL as a value; `concat`, `concat_ws`, `printf` and
/// `format` write it as nothing; `replace(x, '', NULL)` is `x`; and the JSON
/// functions that build or edit a document write it as `null`.
const STRICT_FUNCTIONS: [&str; 37] = [
    "->",
    "->>",
    "abs",
    "glob",
    "instr",
    "json",
    "json_array_length",
    "json_error_position",
    "json_extract",
    "json_patch",
    "json_remove",
    "json_type",
    "json_valid",
    "jsonb",
    "jsonb_extract",
    "jsonb_patch",
    "jsonb_remove",
    "length",
    "like",
    "likelihood",
    "likely",
    "lower",
    "ltrim",
    "max",
    "min",
    "octet_length",
    "round",
    "rtrim",
    "sign",
    "substr",
    "substring",
    "trim",
    "unhex",
    "unicode",
    "unistr",
    "unlikely",
    "upper",
];

/// SQLITE_DETERMINISTIC, as `pragma_function_list` reports it in `flags`.
const DETERMINISTIC: i64 = 0x800;

/// Whether `name` is one of SQLite's date and time functions, which may read
/// the clock.
fn is_clock_function(name: &str) -> bool {
    CLOCK_FUNCTIONS.contains(&name)
}

/// One of the definitions SQLite keeps for a function's name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub(crate) struct Definition {
    /// How many arguments it takes, as `pragma_function_list` reports it: a
    /// count, -1 for any number, -3 for one or more, -4 for two or more.
    arguments: i64,
    /// An aggregate, which a window function is too outside a window.
    aggregate: bool,
    /// A scalar function the connection flags deterministic.
    deterministic: bool,
    /// One of SQLite's own, not one the app or an extension registered.
    builtin: bool,
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
pub(crate) struct Functions {
    definitions: BTreeMap<String, Vec<Definition>>,
    /// Each name asked about, lowercase.
    asked: RefCell<BTreeSet<String>>,
}

impl Functions {
    pub(crate) fn read(connection: &Connection) -> rusqlite::Result<Self> {
        let mut listed = connection
            .prepare_cached("SELECT name, narg, type, flags, builtin FROM pragma_function_list")?;
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
                    builtin: row.get::<_, i64>(4)? != 0,
                },
            ))
        })?;
        for row in rows {
            let (name, definition) = row?;
            functions.entry(name).or_default().push(definition);
        }
        Ok(Self { definitions: functions, asked: RefCell::new(BTreeSet::new()) })
    }

    /// The definitions a call of `name` with `arguments` arguments can run:
    /// those that take exactly that many, or else those that take a number
    /// of arguments that includes it.
    fn called(&self, name: &str, arguments: usize) -> Vec<&Definition> {
        let name = name.to_ascii_lowercase();
        self.asked.borrow_mut().insert(name.clone());
        let Some(definitions) = self.definitions.get(&name) else { return Vec::new() };
        let exact: Vec<&Definition> = definitions
            .iter()
            .filter(|definition| definition.arguments >= 0 && definition.takes(arguments))
            .collect();
        if !exact.is_empty() {
            return exact;
        }
        definitions.iter().filter(|definition| definition.takes(arguments)).collect()
    }

    /// Each name asked about so far, lowercase.
    pub(crate) fn asked(&self) -> BTreeSet<String> {
        self.asked.borrow().clone()
    }

    /// The definitions kept under `name`, lowercase, in order.
    pub(crate) fn definitions(&self, name: &str) -> Vec<Definition> {
        let mut definitions: Vec<Definition> =
            self.definitions.get(name).into_iter().flatten().cloned().collect();
        definitions.sort();
        definitions
    }

    /// Whether a call of `name` with `arguments` arguments, outside any
    /// window, calls an aggregate.
    pub(crate) fn aggregate(&self, name: &str, arguments: usize) -> bool {
        self.called(name, arguments).iter().any(|definition| definition.aggregate)
    }

    /// Whether a call of `name` with `arguments` arguments gives the same
    /// answer each time it runs on the same values: it calls a scalar
    /// function the connection flags deterministic, and none of the date and
    /// time functions, which read the clock for `'now'`.
    pub(crate) fn repeats(&self, name: &str, arguments: usize) -> bool {
        let called = self.called(name, arguments);
        !is_clock_function(&name.to_ascii_lowercase())
            && !called.is_empty()
            && called.iter().all(|definition| definition.deterministic)
    }

    /// Whether a call of `name` with `arguments` arguments returns NULL
    /// whenever one of its arguments is NULL: it calls a built-in scalar
    /// function of `STRICT_FUNCTIONS`, and no function the app registered
    /// under that name.
    pub(crate) fn strict(&self, name: &str, arguments: usize) -> bool {
        let called = self.called(name, arguments);
        STRICT_FUNCTIONS.contains(&name.to_ascii_lowercase().as_str())
            && !called.is_empty()
            && called.iter().all(|definition| definition.builtin && !definition.aggregate)
    }
}
