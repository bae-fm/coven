//! Which SQL expressions never give NULL, which tells whether a scalar
//! subquery does (`SPEC.md`, section 9). SQLite says nothing of this, so the
//! compiler reads the expression's tokens. It knows the forms below, and takes
//! every other expression to be one that can give NULL.

use std::ops::Range;

use super::core::{matching, opens_query, outline, top_level_parts};
use super::lex::{Kind, Token};

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

/// The operators of two or three symbols, each before any that starts it.
const LONG_OPERATORS: [&str; 10] = ["->>", "->", "||", "<=", ">=", "<>", "!=", "==", "<<", ">>"];

/// The operators that bind as `=` does, among those the compiler knows.
const EQUALITIES: [&str; 7] = ["=", "==", "!=", "<>", "is", "isnull", "notnull"];

/// Whether the expression that `tokens` are, all of them, never gives NULL.
/// It takes the operators from the loosest binding to the tightest.
pub(crate) fn expression(sql: &str, tokens: &[Token]) -> bool {
    let operators = operators(sql, tokens);
    // BETWEEN has an AND of its own, which joins no two conditions.
    if operators.iter().any(|operator| operator.text == "between") {
        return false;
    }
    if let Some(parts) = split(tokens, &operators, &["and", "or"]) {
        return parts.into_iter().all(|part| expression(sql, &tokens[part]));
    }
    if tokens.first().is_some_and(|first| first.is_word(sql, "not")) {
        return expression(sql, &tokens[1..]);
    }
    let equality =
        operators.iter().rev().find(|operator| EQUALITIES.contains(&operator.text.as_str()));
    if let Some(last) = equality {
        // IS and the tests for NULL give true or false for any operands.
        return matches!(last.text.as_str(), "is" | "isnull" | "notnull")
            || (expression(sql, &tokens[..last.tokens.start])
                && expression(sql, &tokens[last.tokens.end..]));
    }
    if let Some(parts) = split(tokens, &operators, &["<", "<=", ">", ">="]) {
        return parts.into_iter().all(|part| expression(sql, &tokens[part]));
    }
    let arithmetic = ["+", "-", "*", "/", "%"];
    if operators.iter().any(|operator| arithmetic.contains(&operator.text.as_str())) {
        return integer(sql, tokens);
    }
    if let Some(parts) = split(tokens, &operators, &["||"]) {
        return parts.into_iter().all(|part| expression(sql, &tokens[part]));
    }
    operand(sql, tokens)
}

/// A binary operator that stands outside parentheses and CASE.
struct Operator {
    /// The operator's symbols, or its word in lowercase.
    text: String,
    tokens: Range<usize>,
}

/// The indices of the tokens that stand outside parentheses and CASE, with
/// neither the parentheses nor the CASE and its END.
fn outside<'a>(sql: &'a str, tokens: &'a [Token]) -> impl Iterator<Item = usize> + 'a {
    let mut depth = 0usize;
    tokens.iter().enumerate().filter_map(move |(index, token)| {
        if token.is_symbol(sql, '(') || token.is_word(sql, "case") {
            depth += 1;
        } else if token.is_symbol(sql, ')') || token.is_word(sql, "end") {
            depth = depth.saturating_sub(1);
        } else if depth == 0 {
            return Some(index);
        }
        None
    })
}

/// The binary operators of the expression `tokens` outside parentheses and
/// CASE. A `+` or `-` with no operand before it is a sign, not one of them.
fn operators(sql: &str, tokens: &[Token]) -> Vec<Operator> {
    let words = ["and", "or", "is", "isnull", "notnull", "between"];
    let mut found: Vec<Operator> = Vec::new();
    // The index after the last operator's symbols.
    let mut read = 0;
    for index in outside(sql, tokens) {
        let token = &tokens[index];
        if token.kind == Kind::Word {
            let word = token.text(sql).to_ascii_lowercase();
            if words.contains(&word.as_str()) {
                found.push(Operator { text: word, tokens: index..index + 1 });
            }
        } else if token.kind == Kind::Symbol && index >= read {
            // The lexer gives each symbol of an operator its own token.
            let rest = &sql[token.range.start..];
            let long = LONG_OPERATORS.into_iter().find(|operator| rest.starts_with(operator));
            let text = long.unwrap_or_else(|| token.text(sql));
            // Each symbol of an operator is one byte and one token.
            read = index + text.len();
            let sign = matches!(text, "+" | "-")
                && (index == 0 || found.last().is_some_and(|last| last.tokens.end == index));
            if !sign && text.starts_with(|first| "=!<>+-*/%|&".contains(first)) {
                found.push(Operator { text: text.to_owned(), tokens: index..read });
            }
        }
    }
    found
}

/// The ranges of the operands that the operators `named` join, when the
/// expression has any of them.
fn split(tokens: &[Token], operators: &[Operator], named: &[&str]) -> Option<Vec<Range<usize>>> {
    let mut parts = Vec::new();
    let mut start = 0;
    for operator in operators.iter().filter(|operator| named.contains(&operator.text.as_str())) {
        parts.push(start..operator.tokens.start);
        start = operator.tokens.end;
    }
    if parts.is_empty() {
        return None;
    }
    parts.push(start..tokens.len());
    Some(parts)
}

/// Whether `tokens` hold only counts and whole numbers, with signs and with
/// `+`, `-` and `*` between them. Arithmetic on other numbers can give NULL:
/// infinity minus infinity does, and so does `/` or `%` by zero.
fn integer(sql: &str, tokens: &[Token]) -> bool {
    let operators = operators(sql, tokens);
    if let Some(parts) = split(tokens, &operators, &["+", "-", "*"]) {
        return parts.into_iter().all(|part| integer(sql, &tokens[part]));
    }
    match tokens {
        [number] => {
            number.kind == Kind::Number
                && number.text(sql).bytes().all(|byte| byte.is_ascii_digit())
        }
        [sign, rest @ ..] if sign.is_symbol(sql, '-') || sign.is_symbol(sql, '+') => {
            integer(sql, rest)
        }
        [name, open, ..] if name.is_word(sql, "count") && open.is_symbol(sql, '(') => {
            operand(sql, tokens)
        }
        _ => false,
    }
}

/// Whether the operand that `tokens` are, which no binary operator splits,
/// never gives NULL.
fn operand(sql: &str, tokens: &[Token]) -> bool {
    let last = tokens.len().checked_sub(1);
    match tokens {
        [literal] => matches!(literal.kind, Kind::Number | Kind::Text),
        [sign, rest @ ..] if sign.is_symbol(sql, '-') || sign.is_symbol(sql, '+') => {
            operand(sql, rest)
        }
        [open, inner @ .., _] if open.is_symbol(sql, '(') && matching(sql, tokens, 0) == last => {
            if opens_query(sql, tokens, 0) {
                subquery(sql, inner)
            } else {
                // A row of several values is no value.
                top_level_parts(sql, inner, 0..inner.len()).len() == 1 && expression(sql, inner)
            }
        }
        [exists, open, ..] if exists.is_word(sql, "exists") && open.is_symbol(sql, '(') => {
            matching(sql, tokens, 1) == last
        }
        [case, inner @ .., end] if case.is_word(sql, "case") && end.is_word(sql, "end") => {
            choice(sql, inner)
        }
        [name, open, ..] if name.kind == Kind::Word && open.is_symbol(sql, '(') => {
            call(sql, tokens)
        }
        _ => false,
    }
}

/// Whether the scalar subquery whose query is `tokens` never gives NULL: it
/// returns one row, whose one column never does.
fn subquery(sql: &str, tokens: &[Token]) -> bool {
    let core = outline(sql, tokens);
    match &core.items[..] {
        [item] => core.one_row() && expression(sql, &tokens[item.expression.clone()]),
        _ => false,
    }
}

/// Whether the CASE whose tokens between CASE and END are `tokens` never
/// gives NULL: it has an ELSE, and no THEN or ELSE of it does.
fn choice(sql: &str, tokens: &[Token]) -> bool {
    let keywords: Vec<(&str, usize)> = outside(sql, tokens)
        .filter_map(|index| {
            let words = ["when", "then", "else"];
            let keyword = words.into_iter().find(|word| tokens[index].is_word(sql, word))?;
            Some((keyword, index))
        })
        .collect();
    let ends = keywords.iter().skip(1).map(|&(_, at)| at).chain([tokens.len()]);
    keywords.iter().any(|&(keyword, _)| keyword == "else")
        && keywords
            .iter()
            .zip(ends)
            .filter(|((keyword, _), _)| *keyword != "when")
            .all(|(&(_, at), end)| expression(sql, &tokens[at + 1..end]))
}

/// Whether the function call that `tokens` are, with its FILTER and OVER,
/// never gives NULL.
fn call(sql: &str, tokens: &[Token]) -> bool {
    let Some(close) = matching(sql, tokens, 1) else { return false };
    let mut after = close + 1;
    for clause in ["filter", "over"] {
        if tokens.get(after).is_some_and(|token| token.is_word(sql, clause)) {
            after += 1;
            if tokens.get(after).is_some_and(|open| open.is_symbol(sql, '(')) {
                let Some(end) = matching(sql, tokens, after) else { return false };
                after = end;
            }
            after += 1;
        }
    }
    if after != tokens.len() {
        return false;
    }
    let name = tokens[0].text(sql).to_ascii_lowercase();
    let arguments = top_level_parts(sql, tokens, 2..close);
    let argument = |range: &Range<usize>| expression(sql, &tokens[range.clone()]);
    match (name.as_str(), &arguments[..]) {
        (name, _) if NEVER_NULL_AGGREGATES.contains(&name) || CONSTRUCTORS.contains(&name) => true,
        // `json` gives NULL for NULL alone.
        ("json" | "jsonb", [document]) => argument(document),
        ("coalesce" | "ifnull", values) => values.iter().any(argument),
        ("iif", [_, then, otherwise]) => argument(then) && argument(otherwise),
        _ => false,
    }
}

#[cfg(test)]
#[path = "never_null_tests.rs"]
mod tests;
