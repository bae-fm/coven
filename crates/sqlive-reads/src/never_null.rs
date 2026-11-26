//! Which scalar subqueries never give NULL (sqlive-compose's `SPEC.md`,
//! section 8). SQLite says nothing of this, so the analysis reads the
//! expression a subquery selects. It knows the forms below, and takes every other expression to be
//! one that can give NULL.

use sqlite3_parser::ast::{
    Expr, Literal, OneSelect, Operator, ResultColumn, Select, UnaryOperator,
};

use crate::select_syntax::{aggregate_owners, Functions};
use crate::sql::unparenthesized;

/// The aggregate and window functions that give a value over no rows.
const NEVER_NULL_AGGREGATES: [&str; 6] = [
    "count",
    "total",
    "json_group_array",
    "json_group_object",
    "jsonb_group_array",
    "jsonb_group_object",
];

/// The functions that build a document, whatever their arguments.
const CONSTRUCTORS: [&str; 4] = ["json_object", "jsonb_object", "json_array", "jsonb_array"];

/// Whether the scalar subquery `select` never gives NULL: it returns one row,
/// whose one column never does.
pub(crate) fn subquery(select: &Select<'_>, functions: &Functions) -> bool {
    if select.body.compounds.is_some() || select.limit.is_some() {
        return false;
    }
    let OneSelect::Select { columns, from, group_by: None, having: None, .. } = &select.body.select
    else {
        return false;
    };
    let [ResultColumn::Expr(selected, _)] = &columns[..] else { return false };
    // An aggregate of the subquery's own rows, with no GROUP BY, makes one row
    // of all of them.
    let owners =
        aggregate_owners(columns, from.as_ref(), &mut Vec::new(), functions, &mut Vec::new());
    let one_row = owners.iter().any(Option::is_none);
    one_row && expression(selected, functions)
}

/// Whether `expr` never gives NULL.
fn expression(expr: &Expr<'_>, functions: &Functions) -> bool {
    let never = |expr| expression(expr, functions);
    match unparenthesized(expr) {
        Expr::Literal(Literal::Numeric(_) | Literal::String(_) | Literal::Blob(_)) => true,
        // IS and the tests for NULL give true or false for any operands, and
        // EXISTS for any rows.
        Expr::Binary(_, Operator::Is | Operator::IsNot, _)
        | Expr::IsNull(_)
        | Expr::NotNull(_)
        | Expr::Exists(_) => true,
        Expr::Binary(left, operator, right) => match operator {
            Operator::And
            | Operator::Or
            | Operator::Equals
            | Operator::NotEquals
            | Operator::Less
            | Operator::LessEquals
            | Operator::Greater
            | Operator::GreaterEquals
            | Operator::Concat => never(left) && never(right),
            Operator::Add | Operator::Subtract | Operator::Multiply => integer(expr),
            _ => false,
        },
        Expr::Unary(
            UnaryOperator::Not | UnaryOperator::Negative | UnaryOperator::Positive,
            operand,
        ) => never(operand),
        Expr::Subquery(select) => subquery(select, functions),
        Expr::Case { when_then_pairs, else_expr, .. } => {
            else_expr.is_some_and(never) && when_then_pairs.iter().all(|(_, then)| never(then))
        }
        Expr::FunctionCall { name, args, .. } => call(name.0, args.unwrap_or_default(), functions),
        Expr::FunctionCallStar { name, .. } => call(name.0, &[], functions),
        _ => false,
    }
}

/// Whether `expr` holds only counts and whole numbers, with signs and with
/// `+`, `-` and `*` between them. Arithmetic on other numbers can give NULL:
/// infinity minus infinity does, and so does `/` or `%` by zero.
fn integer(expr: &Expr<'_>) -> bool {
    match unparenthesized(expr) {
        Expr::Binary(left, Operator::Add | Operator::Subtract | Operator::Multiply, right) => {
            integer(left) && integer(right)
        }
        Expr::Literal(Literal::Numeric(number)) => number.bytes().all(|byte| byte.is_ascii_digit()),
        Expr::Unary(UnaryOperator::Negative | UnaryOperator::Positive, operand) => integer(operand),
        Expr::FunctionCall { name, .. } | Expr::FunctionCallStar { name, .. } => {
            name.0.eq_ignore_ascii_case("count")
        }
        _ => false,
    }
}

/// Whether a call of the function `name` with `arguments` never gives NULL.
fn call(name: &str, arguments: &[Expr<'_>], functions: &Functions) -> bool {
    let never = |expr| expression(expr, functions);
    let name = name.to_ascii_lowercase();
    match (name.as_str(), arguments) {
        (name, _) if NEVER_NULL_AGGREGATES.contains(&name) || CONSTRUCTORS.contains(&name) => true,
        // `json` gives NULL for NULL alone.
        ("json" | "jsonb", [document]) => never(document),
        ("coalesce" | "ifnull", values) => values.iter().any(never),
        ("iif", [_, then, otherwise]) => never(then) && never(otherwise),
        _ => false,
    }
}

#[cfg(test)]
#[path = "never_null_tests.rs"]
mod tests;
