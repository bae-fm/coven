//! The outline of a statement's first SELECT core, read from its tokens: the
//! result columns, the FROM items with how each joins, and whether the
//! core aggregates. The compiler reads this much of the SQL itself and leaves
//! the rest to SQLite and sqlive.

use std::ops::Range;

use super::lex::{Kind, Token};

/// How a FROM item joins the items before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Join {
    /// The first item, a comma, or an inner or cross join.
    Inner,
    /// A LEFT join: the item's columns are NULL where no row joins.
    Left,
    /// A RIGHT join.
    Right,
    /// A FULL join.
    Full,
}

/// One item of a FROM clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FromItem {
    /// The name that qualifies its columns: its alias, else its table's name.
    pub(crate) qualifier: Option<String>,
    /// The alias the SQL gives it, lowercase.
    pub(crate) alias: Option<String>,
    /// What it reads.
    pub(crate) source: Source,
    /// How it joins.
    pub(crate) join: Join,
}

/// What a FROM item reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Source {
    /// A table or CTE by its name alone, lowercase, at the token `at`.
    Table { name: String, at: usize },
    /// A table that a schema's name qualifies.
    Qualified { name: String },
    /// A subquery or a table function.
    Derived,
}

impl FromItem {
    /// The table or CTE it reads, lowercase, when it names one.
    pub(crate) fn table(&self) -> Option<&str> {
        match &self.source {
            Source::Table { name, .. } | Source::Qualified { name } => Some(name),
            Source::Derived => None,
        }
    }
}

/// What a result column is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ItemKind {
    /// `*`.
    Star,
    /// `qualifier.*`.
    QualifiedStar(String),
    /// A column: `name` or `qualifier.name`, with no alias or an alias.
    Column { qualifier: Option<String>, name: String },
    /// A subquery in parentheses, which returns one value.
    Subquery,
    /// Any other expression.
    Expression,
}

/// A result column of the core.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Item {
    pub(crate) kind: ItemKind,
    /// The name after `AS`, lowercase.
    pub(crate) alias: Option<String>,
    /// The item's tokens, as indices into the statement's token list.
    pub(crate) tokens: Range<usize>,
}

/// The first SELECT core of a statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Core {
    pub(crate) items: Vec<Item>,
    pub(crate) from: Vec<FromItem>,
    /// Whether a result column calls an aggregate function outside a
    /// subquery.
    pub(crate) aggregates: bool,
    /// Whether the core has a GROUP BY or a HAVING.
    pub(crate) groups: bool,
    /// Whether a compound operator (UNION and the others) follows the core.
    pub(crate) compound: bool,
}

const AGGREGATES: [&str; 12] = [
    "count",
    "sum",
    "total",
    "avg",
    "min",
    "max",
    "group_concat",
    "string_agg",
    "json_group_array",
    "json_group_object",
    "jsonb_group_array",
    "jsonb_group_object",
];

const CLAUSE_ENDS: [&str; 9] =
    ["where", "group", "having", "window", "order", "limit", "union", "intersect", "except"];

const NOT_ALIASES: [&str; 23] = [
    "from",
    "join",
    "left",
    "right",
    "full",
    "inner",
    "outer",
    "cross",
    "natural",
    "on",
    "using",
    "where",
    "group",
    "having",
    "window",
    "order",
    "limit",
    "union",
    "intersect",
    "except",
    "indexed",
    "not",
    "offset",
];

/// The index of the token that closes the parenthesis opened at `open`.
pub(crate) fn matching(sql: &str, tokens: &[Token], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
}

/// The first SELECT core of `sql`, whose tokens are `tokens`.
pub(crate) fn outline(sql: &str, tokens: &[Token]) -> Core {
    let mut core = Core {
        items: Vec::new(),
        from: Vec::new(),
        aggregates: false,
        groups: false,
        compound: false,
    };
    let Some(select) = main_select(sql, tokens) else { return core };
    // The end of each part, at the core's own depth.
    let mut depth = 0usize;
    let mut from_at = None;
    let mut clause_end = tokens.len();
    for (index, token) in tokens.iter().enumerate().skip(select + 1) {
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && token.kind == Kind::Word {
            let word = token.text(sql).to_ascii_lowercase();
            if word == "from" && from_at.is_none() {
                from_at = Some(index);
            } else if CLAUSE_ENDS.contains(&word.as_str()) {
                clause_end = index;
                break;
            }
        }
    }
    let list_end = from_at.unwrap_or(clause_end);
    core.items = result_items(sql, tokens, select + 1..list_end);
    if let Some(from_at) = from_at {
        core.from = from_items(sql, tokens, from_at + 1..clause_end);
    }
    core.aggregates = core.items.iter().any(|item| aggregates(sql, tokens, item.tokens.clone()));
    let mut depth = 0usize;
    for token in &tokens[clause_end.min(tokens.len())..] {
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && token.kind == Kind::Word {
            match token.text(sql).to_ascii_lowercase().as_str() {
                "group" | "having" => core.groups = true,
                "union" | "intersect" | "except" => core.compound = true,
                _ => {}
            }
        }
    }
    core
}

/// The index of the SELECT whose core is the statement's first: after a WITH
/// clause's CTEs, the first SELECT outside parentheses.
fn main_select(sql: &str, tokens: &[Token]) -> Option<usize> {
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && token.is_word(sql, "select") {
            return Some(index);
        }
    }
    None
}

fn result_items(sql: &str, tokens: &[Token], range: Range<usize>) -> Vec<Item> {
    let mut start = range.start;
    if tokens
        .get(start)
        .is_some_and(|token| token.is_word(sql, "distinct") || token.is_word(sql, "all"))
    {
        start += 1;
    }
    let mut items = Vec::new();
    let mut depth = 0usize;
    let mut item_start = start;
    for index in start..range.end {
        let token = &tokens[index];
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && token.is_symbol(sql, ',') {
            items.push(item(sql, tokens, item_start..index));
            item_start = index + 1;
        }
    }
    if item_start < range.end {
        items.push(item(sql, tokens, item_start..range.end));
    }
    items
}

fn item(sql: &str, tokens: &[Token], range: Range<usize>) -> Item {
    let parts = &tokens[range.clone()];
    let (expression, alias) = match parts {
        [.., before, last] if before.is_word(sql, "as") => {
            (&parts[..parts.len() - 2], last.name(sql))
        }
        _ => (parts, None),
    };
    let kind = match expression {
        [star] if star.is_symbol(sql, '*') => ItemKind::Star,
        [qualifier, dot, star]
            if dot.is_symbol(sql, '.')
                && star.is_symbol(sql, '*')
                && qualifier.name(sql).is_some() =>
        {
            ItemKind::QualifiedStar(qualifier.name(sql).unwrap_or_default())
        }
        [name] if name.name(sql).is_some() => {
            ItemKind::Column { qualifier: None, name: name.name(sql).unwrap_or_default() }
        }
        [qualifier, dot, name]
            if dot.is_symbol(sql, '.')
                && qualifier.name(sql).is_some()
                && name.name(sql).is_some() =>
        {
            ItemKind::Column {
                qualifier: qualifier.name(sql),
                name: name.name(sql).unwrap_or_default(),
            }
        }
        [open, select, .., close]
            if open.is_symbol(sql, '(')
                && select.is_word(sql, "select")
                && close.is_symbol(sql, ')')
                && matching(sql, expression, 0) == Some(expression.len() - 1) =>
        {
            ItemKind::Subquery
        }
        _ => ItemKind::Expression,
    };
    Item { kind, alias, tokens: range }
}

fn from_items(sql: &str, tokens: &[Token], range: Range<usize>) -> Vec<FromItem> {
    let mut items = Vec::new();
    let mut index = range.start;
    let mut join = Join::Inner;
    let mut depth = 0usize;
    let mut expecting_table = true;
    while index < range.end {
        let token = &tokens[index];
        if depth == 0 && expecting_table {
            expecting_table = false;
            let (mut item, next) = from_item(sql, tokens, index, range.end, join);
            if let Some(next_index) = next {
                item.join = join;
                items.push(item);
                join = Join::Inner;
                index = next_index;
                continue;
            }
        }
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 {
            if token.is_symbol(sql, ',') {
                join = Join::Inner;
                expecting_table = true;
            } else if token.kind == Kind::Word {
                match token.text(sql).to_ascii_lowercase().as_str() {
                    "left" => join = Join::Left,
                    "right" => join = Join::Right,
                    "full" => join = Join::Full,
                    "inner" | "cross" | "natural" => join = Join::Inner,
                    "join" => expecting_table = true,
                    _ => {}
                }
            }
        }
        index += 1;
    }
    items
}

/// The alias that the tokens from `at` up to `end` give the item before
/// them, and how many tokens it takes: `AS name`, or a name that no keyword
/// of the SQL could be.
pub(crate) fn alias_at(
    sql: &str,
    tokens: &[Token],
    at: usize,
    end: usize,
) -> Option<(String, usize)> {
    let after = tokens.get(at).filter(|_| at < end)?;
    if after.is_word(sql, "as") {
        return tokens.get(at + 1).and_then(|alias| alias.name(sql)).map(|alias| (alias, 2));
    }
    let alias = after.name(sql)?;
    let keyword = after.kind == Kind::Word && NOT_ALIASES.contains(&alias.as_str());
    (!keyword).then_some((alias, 1))
}

/// Every FROM item of the statement, at any depth.
pub(crate) fn all_from_items(sql: &str, tokens: &[Token]) -> Vec<FromItem> {
    let mut items = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        let distinct = index > 0 && tokens[index - 1].is_word(sql, "distinct");
        if token.is_word(sql, "from") && !distinct {
            items.extend(from_items(sql, tokens, index + 1..clause_end(sql, tokens, index)));
        }
    }
    items
}

/// The index where the clause that the FROM at `from` starts ends: at the
/// next clause of its SELECT, or the parenthesis that closes its subquery.
fn clause_end(sql: &str, tokens: &[Token], from: usize) -> usize {
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(from + 1) {
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            if depth == 0 {
                return index;
            }
            depth -= 1;
        } else if depth == 0
            && token.kind == Kind::Word
            && CLAUSE_ENDS.contains(&token.text(sql).to_ascii_lowercase().as_str())
        {
            return index;
        }
    }
    tokens.len()
}

/// The FROM item that starts at `index`, and where the next token after it
/// is, or `None` when the tokens there aren't an item.
fn from_item(
    sql: &str,
    tokens: &[Token],
    index: usize,
    end: usize,
    join: Join,
) -> (FromItem, Option<usize>) {
    let mut item = FromItem { qualifier: None, alias: None, source: Source::Derived, join };
    let token = &tokens[index];
    let mut next = index + 1;
    if token.is_symbol(sql, '(') {
        let Some(close) = matching(sql, tokens, index) else { return (item, None) };
        next = close + 1;
    } else if let Some(name) = token.name(sql) {
        let after = tokens.get(next);
        if after.is_some_and(|after| after.is_symbol(sql, '.')) {
            // `schema.table`.
            let Some(table) = tokens.get(next + 1).and_then(|table| table.name(sql)) else {
                return (item, None);
            };
            item.source = Source::Qualified { name: table };
            next += 2;
        } else if after.is_some_and(|after| after.is_symbol(sql, '(')) {
            let Some(close) = matching(sql, tokens, next) else { return (item, None) };
            next = close + 1;
        } else {
            item.source = Source::Table { name, at: index };
        }
    } else {
        return (item, None);
    }
    if let Some((alias, used)) = alias_at(sql, tokens, next, end) {
        item.alias = Some(alias);
        next += used;
    }
    item.qualifier = item.alias.clone().or_else(|| item.table().map(str::to_owned));
    (item, Some(next))
}

/// Whether the tokens in `range` call an aggregate function outside a
/// subquery.
fn aggregates(sql: &str, tokens: &[Token], range: Range<usize>) -> bool {
    let mut index = range.start;
    while index < range.end {
        let token = &tokens[index];
        if token.is_symbol(sql, '(')
            && tokens.get(index + 1).is_some_and(|next| next.is_word(sql, "select"))
        {
            match matching(sql, tokens, index) {
                Some(close) => {
                    index = close + 1;
                    continue;
                }
                None => return false,
            }
        }
        let opens = tokens.get(index + 1).is_some_and(|next| next.is_symbol(sql, '('));
        if token.kind == Kind::Word && opens {
            let name = token.text(sql).to_ascii_lowercase();
            if AGGREGATES.contains(&name.as_str()) {
                if let Some(close) = matching(sql, tokens, index + 1) {
                    let scalar = matches!(name.as_str(), "min" | "max")
                        && arguments(sql, tokens, index + 1, close) > 1;
                    let mut after = close + 1;
                    if tokens.get(after).is_some_and(|next| next.is_word(sql, "filter")) {
                        after = matching(sql, tokens, after + 1).map_or(after, |close| close + 1);
                    }
                    let window = tokens.get(after).is_some_and(|next| next.is_word(sql, "over"));
                    if !scalar && !window {
                        return true;
                    }
                }
            }
        }
        index += 1;
    }
    false
}

/// How many arguments the call whose parentheses are at `open` and `close`
/// has.
pub(crate) fn arguments(sql: &str, tokens: &[Token], open: usize, close: usize) -> usize {
    if close == open + 1 {
        return 0;
    }
    let mut depth = 0usize;
    let mut count = 1;
    for token in &tokens[open + 1..close] {
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && token.is_symbol(sql, ',') {
            count += 1;
        }
    }
    count
}

#[cfg(test)]
#[path = "core_tests.rs"]
mod tests;
