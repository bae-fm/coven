//! The parsed SELECT a scope plan reads: its parameters numbered in source
//! order, and the expressions inside it (§4).

use rusqlite::Statement;
use sqlite3_parser::ast::{
    Expr, FunctionCallOrder, FunctionTail, OneSelect, Operator, Over, ResultColumn, Select,
    SelectTable, Window,
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
            digits
                .parse::<usize>()
                .expect("SQLite accepted this parameter index")
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

/// Whether the SELECT holds another SELECT, or reads a table through `IN`,
/// anywhere: its rows aren't the outer statement's.
pub(crate) fn nests(select: &Select<'_>) -> bool {
    let OneSelect::Select {
        columns,
        from,
        where_clause,
        group_by,
        having,
        window_clause,
        ..
    } = &select.body.select
    else {
        return true;
    };
    let from_nests = from.as_ref().is_some_and(|from| {
        from.select
            .iter()
            .copied()
            .chain(from.joins.iter().flatten().map(|join| &join.table))
            .any(|table| match table {
                SelectTable::Table(..) => false,
                SelectTable::TableCall(_, args, _) => args.is_some_and(|args| args.iter().any(nested)),
                SelectTable::Select(..) | SelectTable::Sub(..) => true,
            })
            || from.joins.iter().flatten().any(|join| {
                matches!(&join.constraint, Some(sqlite3_parser::ast::JoinConstraint::On(expr)) if nested(expr))
            })
    });
    from_nests
        || columns
            .iter()
            .any(|column| matches!(column, ResultColumn::Expr(expr, _) if nested(expr)))
        || where_clause.is_some_and(nested)
        || group_by.is_some_and(|exprs| exprs.iter().any(nested))
        || having.is_some_and(nested)
        || window_clause.is_some_and(|windows| windows.iter().any(|def| window_nests(&def.window)))
        || select
            .order_by
            .is_some_and(|terms| terms.iter().any(|term| nested(&term.expr)))
        || select
            .limit
            .is_some_and(|limit| nested(&limit.expr) || limit.offset.as_ref().is_some_and(nested))
}

fn window_nests(window: &Window<'_>) -> bool {
    window
        .partition_by
        .is_some_and(|exprs| exprs.iter().any(nested))
        || window
            .order_by
            .is_some_and(|terms| terms.iter().any(|term| nested(&term.expr)))
        || window.frame_clause.as_ref().is_some_and(|frame| {
            [Some(&frame.start), frame.end.as_ref()]
                .into_iter()
                .flatten()
                .any(|bound| match bound {
                    sqlite3_parser::ast::FrameBound::Following(expr)
                    | sqlite3_parser::ast::FrameBound::Preceding(expr) => nested(expr),
                    _ => false,
                })
        })
}

fn tail_nests(tail: &FunctionTail<'_>) -> bool {
    tail.filter_clause.is_some_and(nested)
        || tail.over_clause.is_some_and(|over| match over {
            Over::Window(window) => window_nests(window),
            Over::Name(_) => false,
        })
}

/// Whether `expr` holds a SELECT or reads a table through `IN`.
pub(crate) fn nested(expr: &Expr<'_>) -> bool {
    match expr {
        Expr::Exists(_) | Expr::InSelect { .. } | Expr::Subquery(_) | Expr::InTable { .. } => true,
        Expr::Between {
            lhs, start, end, ..
        } => nested(lhs) || nested(start) || nested(end),
        Expr::Binary(left, _, right) => nested(left) || nested(right),
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            base.is_some_and(nested)
                || when_then_pairs
                    .iter()
                    .any(|(when, then)| nested(when) || nested(then))
                || else_expr.is_some_and(nested)
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => nested(expr),
        Expr::FunctionCall {
            args,
            order_by,
            filter_over,
            ..
        } => {
            args.is_some_and(|args| args.iter().any(nested))
                || order_by.as_ref().is_some_and(|order| match order {
                    FunctionCallOrder::SortList(terms) => {
                        terms.iter().any(|term| nested(&term.expr))
                    }
                })
                || filter_over.as_ref().is_some_and(tail_nests)
        }
        Expr::FunctionCallStar { filter_over, .. } => filter_over.as_ref().is_some_and(tail_nests),
        Expr::InList { lhs, rhs, .. } => {
            nested(lhs) || rhs.is_some_and(|exprs| exprs.iter().any(nested))
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => nested(lhs) || nested(rhs) || escape.is_some_and(nested),
        Expr::Parenthesized(exprs) => exprs.iter().any(nested),
        Expr::Raise(_, expr) => expr.is_some_and(nested),
        Expr::Variable(_)
        | Expr::DoublyQualified(..)
        | Expr::Id(_)
        | Expr::Literal(_)
        | Expr::Name(_)
        | Expr::Qualified(..) => false,
    }
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

/// Every name in `expr` that SQLite could resolve to a column: bare,
/// qualified by a table, or by a schema and a table.
pub(crate) fn names<'a>(expr: &'a Expr<'a>, into: &mut Vec<&'a Expr<'a>>) {
    match expr {
        Expr::Id(_) | Expr::Name(_) | Expr::Qualified(..) | Expr::DoublyQualified(..) => {
            into.push(expr)
        }
        Expr::Between {
            lhs, start, end, ..
        } => {
            names(lhs, into);
            names(start, into);
            names(end, into);
        }
        Expr::Binary(left, _, right) => {
            names(left, into);
            names(right, into);
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                names(base, into);
            }
            for (when, then) in when_then_pairs.iter() {
                names(when, into);
                names(then, into);
            }
            if let Some(other) = else_expr {
                names(other, into);
            }
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => names(expr, into),
        Expr::FunctionCall { args, .. } => {
            for arg in args.iter().flat_map(|args| args.iter()) {
                names(arg, into);
            }
        }
        Expr::InList { lhs, rhs, .. } => {
            names(lhs, into);
            for expr in rhs.iter().flat_map(|exprs| exprs.iter()) {
                names(expr, into);
            }
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            names(lhs, into);
            names(rhs, into);
            if let Some(escape) = escape {
                names(escape, into);
            }
        }
        Expr::Parenthesized(exprs) => {
            for expr in exprs.iter() {
                names(expr, into);
            }
        }
        Expr::Raise(_, expr) => {
            if let Some(expr) = expr {
                names(expr, into);
            }
        }
        // A window or filter's own names, and nested SELECTs, belong to
        // what nests() already refused or to no row of a source.
        Expr::Exists(_)
        | Expr::InSelect { .. }
        | Expr::Subquery(_)
        | Expr::InTable { .. }
        | Expr::FunctionCallStar { .. }
        | Expr::Literal(_)
        | Expr::Variable(_) => {}
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
