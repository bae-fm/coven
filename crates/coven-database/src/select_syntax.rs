//! The parsed SELECT a scope plan reads: its parameters numbered in source
//! order, and the expressions inside it (§4).

use rusqlite::Statement;
use sqlite3_parser::ast::{
    Expr, FrameBound, FunctionCallOrder, FunctionTail, Operator, Over, Select, Window,
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
    match expr {
        Expr::Binary(left, Operator::And, right) => {
            conjuncts(left, into);
            conjuncts(right, into);
        }
        Expr::Parenthesized(exprs) if exprs.len() == 1 => conjuncts(&exprs[0], into),
        _ => into.push(expr),
    }
}

/// What an expression holds, outside the SELECTs nested in it.
pub(crate) enum Part<'a> {
    /// A name SQLite could resolve to a column: bare, qualified by a table,
    /// or by a schema and a table.
    Name(&'a Expr<'a>),
    /// A SELECT, with sources of its own.
    Select(&'a Select<'a>),
    /// `x IN table`: a table read outside any FROM.
    Table,
    /// A COLLATE, which gives a comparison holding it its collation.
    Collation,
    /// A function called by name, and whether it runs over a window.
    Function(&'a str, bool),
}

/// The parts of `expr`, in every operand, argument, filter and window.
pub(crate) fn parts<'a>(expr: &'a Expr<'a>, into: &mut Vec<Part<'a>>) {
    match expr {
        Expr::Id(_) | Expr::Name(_) | Expr::Qualified(..) | Expr::DoublyQualified(..) => {
            into.push(Part::Name(expr))
        }
        Expr::Exists(select) | Expr::Subquery(select) => into.push(Part::Select(select)),
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

/// `name` as an SQL identifier.
pub(crate) fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
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
