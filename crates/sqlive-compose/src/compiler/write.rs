//! What the compiler reads of a write's SQL itself: the table it changes, its
//! RETURNING columns, and the parameters that stand alone as a value of a
//! NOT NULL column (`SPEC.md`, section 8). SQLite and the schema check the
//! rest of the statement.

use super::core::{alias_at, matching, result_items, Core, FromItem, Item, Source};
use super::error::Cause;
use super::header::Header;
use super::lex::{Kind, Token};

/// The table a write changes.
pub(crate) struct Target {
    /// What the write does to it.
    verb: Verb,
    /// The table as a FROM item reads it: its alias, else its name. A spread
    /// that reads a parent row may name it, since the write changes rows of
    /// the table.
    pub(crate) item: FromItem,
    /// The index of the token after the table, and the alias that follows it.
    after: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Verb {
    Insert,
    Update,
    Delete,
}

/// The table that the INSERT, REPLACE, UPDATE or DELETE `tokens` hold
/// changes, or `None` when they are no such statement. A WITH clause may come
/// first.
pub(crate) fn target(sql: &str, tokens: &[Token]) -> Option<Target> {
    let keyword = statement_keyword(sql, tokens)?;
    let word = tokens[keyword].text(sql).to_ascii_lowercase();
    let or_action = |at: usize| {
        if tokens.get(at).is_some_and(|or| or.is_word(sql, "or")) {
            at + 2
        } else {
            at
        }
    };
    let (verb, table) = match word.as_str() {
        "insert" => {
            let into = or_action(keyword + 1);
            tokens.get(into).filter(|token| token.is_word(sql, "into"))?;
            (Verb::Insert, into + 1)
        }
        "replace" => {
            tokens.get(keyword + 1).filter(|token| token.is_word(sql, "into"))?;
            (Verb::Insert, keyword + 2)
        }
        "update" => (Verb::Update, or_action(keyword + 1)),
        "delete" => {
            tokens.get(keyword + 1).filter(|token| token.is_word(sql, "from"))?;
            (Verb::Delete, keyword + 2)
        }
        _ => return None,
    };
    let (name, at, mut after) = table_name(sql, tokens, table)?;
    let mut alias = None;
    if verb == Verb::Insert {
        // An INSERT's alias follows AS, so that VALUES and SELECT are no alias.
        if tokens.get(after).is_some_and(|token| token.is_word(sql, "as")) {
            alias = tokens.get(after + 1).and_then(|token| token.name(sql));
            after += 2;
        }
    } else if let Some((found, used)) = alias_at(sql, tokens, after, tokens.len()) {
        alias = Some(found);
        after += used;
    }
    let qualified = tokens.get(table + 1).is_some_and(|dot| dot.is_symbol(sql, '.'));
    let source = if qualified {
        Source::Qualified { name: name.clone() }
    } else {
        Source::Table { name: name.clone(), at }
    };
    let item = FromItem {
        qualifier: alias.clone().or(Some(name)),
        alias,
        source,
        join: super::core::Join::Inner,
    };
    Some(Target { verb, item, after })
}

/// The index of the word that starts the statement: the first token, or the
/// first of the words that start a statement at the depth of the first token
/// after a WITH clause.
fn statement_keyword(sql: &str, tokens: &[Token]) -> Option<usize> {
    if !tokens.first()?.is_word(sql, "with") {
        return Some(0);
    }
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0
            && ["select", "values", "insert", "replace", "update", "delete"]
                .iter()
                .any(|word| token.is_word(sql, word))
        {
            return Some(index);
        }
    }
    None
}

/// The table named at `at`, which a schema's name may qualify: its name, the
/// index of the token that holds it, and the index after it.
fn table_name(sql: &str, tokens: &[Token], at: usize) -> Option<(String, usize, usize)> {
    let first = tokens.get(at)?.name(sql)?;
    if tokens.get(at + 1).is_some_and(|dot| dot.is_symbol(sql, '.')) {
        let table = tokens.get(at + 2)?.name(sql)?;
        return Some((table, at + 2, at + 3));
    }
    Some((first, at, at + 1))
}

/// The RETURNING items of the statement as the items of a core that reads the
/// table the write changes, so that the compiler types them as it types a
/// read's result columns.
pub(crate) fn returning(sql: &str, tokens: &[Token], target: FromItem) -> Core {
    let mut depth = 0usize;
    let mut items = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && token.is_word(sql, "returning") {
            items = result_items(sql, tokens, index + 1..tokens.len());
            break;
        }
    }
    Core { items, from: vec![target], aggregates: false, groups: false, compound: false }
}

/// Fails when a parameter that can be NULL is the whole value of a NOT NULL
/// column: a VALUES item or a first SELECT's item of an INSERT, and a value
/// of SET in an UPDATE or in an upsert's DO UPDATE. SQLite prepares such a
/// statement and refuses it only when it runs with a NULL.
pub(crate) fn check_not_null(
    header: &Header,
    sql: &str,
    tokens: &[Token],
    target: &Target,
    columns: &[sqlive_reads::TableColumn],
    select_items: &[Item],
) -> Result<(), Cause> {
    let mut values = Vec::new();
    match target.verb {
        Verb::Insert => {
            let (listed, next) = column_list(sql, tokens, target.after);
            let names = listed.unwrap_or_else(|| {
                columns.iter().map(|column| column.name.to_ascii_lowercase()).collect()
            });
            inserted_values(sql, tokens, next, &names, select_items, &mut values);
            values.extend(assignments_after(sql, tokens, 0, &["do", "update", "set"]));
        }
        Verb::Update => values.extend(assignments_after(sql, tokens, target.after, &["set"])),
        Verb::Delete => {}
    }
    for (column, parameter) in values.iter().map(|(column, parameter)| (column.as_str(), parameter))
    {
        let not_null =
            columns.iter().any(|known| known.not_null && known.name.eq_ignore_ascii_case(column));
        let declared = header.parameters.iter().find(|declared| declared.name == *parameter);
        if not_null && declared.is_some_and(|declared| declared.ty.is_option()) {
            return Err(Cause::NullIntoNotNull {
                concerns: header.name.clone(),
                parameter: parameter.clone(),
                column: column.to_owned(),
            });
        }
    }
    Ok(())
}

/// The name of the parameter `:name` that the tokens in `range` are, with no
/// other token but an alias after AS.
fn lone_parameter(sql: &str, tokens: &[Token], range: std::ops::Range<usize>) -> Option<String> {
    let parts = &tokens[range];
    let (first, rest) = parts.split_first()?;
    let alias = rest.is_empty() || (rest.len() == 2 && rest[0].is_word(sql, "as"));
    let text = first.text(sql);
    (first.kind == Kind::Parameter && alias && text.starts_with(':')).then(|| text[1..].to_owned())
}

/// The column list in parentheses at `at`, lowercase, when there is one, and
/// the index after it.
fn column_list(sql: &str, tokens: &[Token], at: usize) -> (Option<Vec<String>>, usize) {
    if !tokens.get(at).is_some_and(|open| open.is_symbol(sql, '(')) {
        return (None, at);
    }
    let Some(close) = matching(sql, tokens, at) else { return (None, at) };
    let names = tokens[at + 1..close].iter().filter_map(|token| token.name(sql)).collect();
    (Some(names), close + 1)
}

/// The values an INSERT's VALUES rows or first SELECT give the columns
/// `names`, by position.
fn inserted_values(
    sql: &str,
    tokens: &[Token],
    mut at: usize,
    names: &[String],
    select_items: &[Item],
    values: &mut Vec<(String, String)>,
) {
    if tokens.get(at).is_some_and(|token| token.is_word(sql, "values")) {
        at += 1;
        while tokens.get(at).is_some_and(|open| open.is_symbol(sql, '(')) {
            let Some(close) = matching(sql, tokens, at) else { return };
            for (name, range) in names.iter().zip(split(sql, tokens, at + 1..close)) {
                if let Some(parameter) = lone_parameter(sql, tokens, range) {
                    values.push((name.clone(), parameter));
                }
            }
            at = close + 1;
            if tokens.get(at).is_some_and(|comma| comma.is_symbol(sql, ',')) {
                at += 1;
            }
        }
        return;
    }
    if tokens.get(at).is_some_and(|token| token.is_word(sql, "select")) {
        for (name, item) in names.iter().zip(select_items) {
            if matches!(
                item.kind,
                super::core::ItemKind::Star | super::core::ItemKind::QualifiedStar(_)
            ) {
                return;
            }
            if let Some(parameter) = lone_parameter(sql, tokens, item.tokens.clone()) {
                values.push((name.clone(), parameter));
            }
        }
    }
}

/// The ranges of the comma-separated parts of `range`, at its own depth.
fn split(
    sql: &str,
    tokens: &[Token],
    range: std::ops::Range<usize>,
) -> Vec<std::ops::Range<usize>> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = range.start;
    for index in range.clone() {
        if tokens[index].is_symbol(sql, '(') {
            depth += 1;
        } else if tokens[index].is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && tokens[index].is_symbol(sql, ',') {
            parts.push(start..index);
            start = index + 1;
        }
    }
    if start < range.end {
        parts.push(start..range.end);
    }
    parts
}

/// The columns that `column = :parameter` assigns in the SET that follows
/// the words `after`, from the index `from` on.
fn assignments_after(
    sql: &str,
    tokens: &[Token],
    from: usize,
    after: &[&str],
) -> Vec<(String, String)> {
    let mut depth = 0usize;
    let mut start = None;
    for index in from..tokens.len() {
        if tokens[index].is_symbol(sql, '(') {
            depth += 1;
        } else if tokens[index].is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && index + after.len() <= tokens.len() {
            let here = after
                .iter()
                .enumerate()
                .all(|(offset, word)| tokens[index + offset].is_word(sql, word));
            if here {
                start = Some(index + after.len());
                break;
            }
        }
    }
    let Some(start) = start else { return Vec::new() };
    let mut end = tokens.len();
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(start) {
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0
            && ["from", "where", "returning", "order", "limit"]
                .iter()
                .any(|word| token.is_word(sql, word))
        {
            end = index;
            break;
        }
    }
    let mut found = Vec::new();
    for part in split(sql, tokens, start..end) {
        let [column, equals, ..] = &tokens[part.clone()] else { continue };
        let (Some(column), true) = (column.name(sql), equals.is_symbol(sql, '=')) else { continue };
        if let Some(parameter) = lone_parameter(sql, tokens, part.start + 2..part.end) {
            found.push((column, parameter));
        }
    }
    found
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
