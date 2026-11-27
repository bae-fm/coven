//! Renaming the CTEs a call declares, and the relations a fragment is given,
//! to `<crate>__<fragment>__<name>`, so that no two fragments' CTEs share a
//! name once their SQL is one statement (`SPEC.md`, section 4).

use std::collections::BTreeSet;
use std::ops::Range;

use super::core::{all_from_items, matching, Source};
use super::lex::{self, Token};

/// A CTE the SQL declares.
struct Declared {
    name: String,
    /// The index of the token that holds its name.
    at: usize,
    /// The indices of the tokens of its body, inside its parentheses.
    body: Range<usize>,
    /// Whether its WITH clause is RECURSIVE, so the body may name the CTE.
    recursive: bool,
}

/// The name that `prefix` gives the CTE or relation `name`.
pub(crate) fn renamed(prefix: &str, name: &str) -> String {
    format!("{prefix}__{}", name.to_ascii_lowercase())
}

/// `sql` with each CTE it declares, and each of `relations` it reads as a
/// table, named by `prefix`. A table read without an alias keeps its old
/// name as one, so the columns the SQL qualifies by it still resolve.
pub(crate) fn rename(sql: &str, prefix: &str, relations: &[String]) -> String {
    lex::apply(sql, &edits(sql, prefix, relations))
}

/// The edits by which `rename` renames.
pub(crate) fn edits(sql: &str, prefix: &str, relations: &[String]) -> Vec<(Range<usize>, String)> {
    let tokens = lex::tokens(sql);
    let declared = declarations(sql, &tokens);
    let names = renamed_names(sql, relations);
    if names.is_empty() {
        return Vec::new();
    }
    let mut edits = Vec::new();
    for cte in &declared {
        edits.push((tokens[cte.at].range.clone(), renamed(prefix, &cte.name)));
    }
    for item in all_from_items(sql, &tokens) {
        let Source::Table { name: table, at } = &item.source else { continue };
        if !names.contains(table) {
            continue;
        }
        let inside_own_body = declared
            .iter()
            .any(|cte| !cte.recursive && cte.name == *table && cte.body.contains(at));
        if inside_own_body {
            continue;
        }
        let original = tokens[*at].text(sql);
        let new = renamed(prefix, table);
        let text = if item.alias.is_some() { new } else { format!("{new} AS {original}") };
        edits.push((tokens[*at].range.clone(), text));
    }
    edits
}

/// The names `rename` renames, lowercase: the CTEs the SQL declares, at any
/// depth, and `relations`.
pub(crate) fn renamed_names(sql: &str, relations: &[String]) -> BTreeSet<String> {
    let tokens = lex::tokens(sql);
    let mut names: BTreeSet<String> =
        declarations(sql, &tokens).into_iter().map(|cte| cte.name).collect();
    names.extend(relations.iter().map(|relation| relation.to_ascii_lowercase()));
    names
}

/// The CTEs the SQL declares, at any depth.
fn declarations(sql: &str, tokens: &[Token]) -> Vec<Declared> {
    let mut declared = Vec::new();
    for (start, token) in tokens.iter().enumerate() {
        if !token.is_word(sql, "with") {
            continue;
        }
        let mut at = start + 1;
        let recursive = tokens.get(at).is_some_and(|next| next.is_word(sql, "recursive"));
        if recursive {
            at += 1;
        }
        while let Some(name) = tokens.get(at).and_then(|name| name.name(sql)) {
            let name_at = at;
            at += 1;
            if tokens.get(at).is_some_and(|next| next.is_symbol(sql, '(')) {
                let Some(close) = matching(sql, tokens, at) else { return declared };
                at = close + 1;
            }
            if !tokens.get(at).is_some_and(|next| next.is_word(sql, "as")) {
                break;
            }
            at += 1;
            while tokens
                .get(at)
                .is_some_and(|next| next.is_word(sql, "not") || next.is_word(sql, "materialized"))
            {
                at += 1;
            }
            if !tokens.get(at).is_some_and(|next| next.is_symbol(sql, '(')) {
                break;
            }
            let Some(close) = matching(sql, tokens, at) else { return declared };
            declared.push(Declared { name, at: name_at, body: at + 1..close, recursive });
            at = close + 1;
            if tokens.get(at).is_some_and(|next| next.is_symbol(sql, ',')) {
                at += 1;
            } else {
                break;
            }
        }
    }
    declared
}

#[cfg(test)]
#[path = "rename_tests.rs"]
mod tests;
