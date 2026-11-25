//! Spreads: the `@path::Fragment` markers of a call's SQL, replaced by the
//! SQL of the fragments they name (`SPEC.md`, sections 3 and 4).

use std::collections::BTreeMap;
use std::ops::Range;

use super::core::{alias_at, matching};
use super::error::Cause;
use super::lex::{self, Kind, Token};
use super::model::Record;
use super::rename::renamed;

/// The fragments a call may spread, by the Rust path of their types.
pub(crate) struct Registry {
    crate_path: String,
    records: BTreeMap<String, Record>,
}

impl Registry {
    /// A registry for the crate `crate_name`, with no fragments.
    pub(crate) fn new(crate_name: &str) -> Self {
        Self { crate_path: crate_name.replace('-', "_"), records: BTreeMap::new() }
    }

    /// The path that names a fragment in this registry: `crate::…` stands for
    /// this crate's own path.
    pub(crate) fn key(&self, written: &str) -> String {
        match written.strip_prefix("crate::") {
            Some(rest) => format!("{}::{rest}", self.crate_path),
            None => written.replace('-', "_"),
        }
    }

    /// Offer `record` to the spreads.
    pub(crate) fn add(&mut self, record: Record) {
        self.records.insert(record.path.clone(), record);
    }

    fn get(&self, key: &str) -> Option<&Record> {
        self.records.get(key)
    }

    /// How a call's own code writes the path of a fragment type: `crate::…`
    /// for a fragment of this crate.
    pub(crate) fn code_path(&self, key: &str) -> String {
        match key.strip_prefix(self.crate_path.as_str()) {
            Some(rest) if rest.starts_with("::") => format!("crate{rest}"),
            _ => key.to_owned(),
        }
    }
}

/// A fragment spread where the SQL stands.
pub(crate) struct Placed {
    /// The alias the SQL gives it.
    pub(crate) alias: Option<String>,
    /// The fragment.
    pub(crate) record: Record,
    /// The alias, lowercase, of the FROM item that gives the parent row,
    /// when the fragment reads one.
    pub(crate) parent: Option<String>,
}

/// A call's SQL with its spreads expanded.
pub(crate) struct Expansion {
    pub(crate) sql: String,
    pub(crate) placed: Vec<Placed>,
}

/// The paths the markers of `sql` name, as written, and not `@parent`.
pub(crate) fn spread_paths(sql: &str) -> Vec<String> {
    lex::tokens(sql)
        .iter()
        .filter(|token| token.kind == Kind::Marker)
        .map(|token| token.text(sql)[1..].to_owned())
        .filter(|path| path != "parent")
        .collect()
}

/// What one argument of a spread binds.
struct Argument {
    name: String,
    /// The tokens' text, from the first to the last.
    value: String,
    /// The one token the value is, when it is one.
    single: Option<Token>,
    /// The text of `value`'s tokens, for the name or parameter it is.
    single_text: Option<String>,
}

/// `sql` with each spread replaced by the SQL of its fragment as a subquery.
/// `takes_parent` says that the SQL belongs to a fragment that reads a parent
/// row, so `@parent.column` is its marker for one.
pub(crate) fn expand(
    registry: &Registry,
    concerns: &str,
    takes_parent: bool,
    sql: &str,
) -> Result<Expansion, Cause> {
    let tokens = lex::tokens(sql);
    let mut edits = Vec::new();
    let mut placed = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let token = &tokens[index];
        if token.kind != Kind::Marker {
            index += 1;
            continue;
        }
        let written = &token.text(sql)[1..];
        if written == "parent" {
            let named = tokens.get(index + 2).is_some_and(|column| column.name(sql).is_some());
            if !takes_parent {
                return Err(Cause::ParentOutsideFragment { concerns: concerns.to_owned() });
            }
            if !tokens.get(index + 1).is_some_and(|dot| dot.is_symbol(sql, '.')) || !named {
                return Err(Cause::StrayMarker {
                    concerns: concerns.to_owned(),
                    text: "@parent".to_owned(),
                });
            }
            index += 3;
            continue;
        }
        if !written.contains("::") {
            return Err(Cause::StrayMarker {
                concerns: concerns.to_owned(),
                text: token.text(sql).to_owned(),
            });
        }
        let key = registry.key(written);
        let record = registry.get(&key).ok_or_else(|| Cause::UnknownFragment {
            concerns: concerns.to_owned(),
            path: written.to_owned(),
        })?;
        let mut end = token.range.end;
        let mut next = index + 1;
        let mut arguments = Vec::new();
        if tokens.get(next).is_some_and(|open| open.is_symbol(sql, '(')) {
            let close = matching(sql, &tokens, next).ok_or_else(|| Cause::BadSpread {
                concerns: concerns.to_owned(),
                fragment: key.clone(),
            })?;
            arguments = arguments_of(sql, &tokens, next + 1..close, concerns, &key)?;
            end = tokens[close].range.end;
            next = close + 1;
        }
        let alias = alias_at(sql, &tokens, next, tokens.len()).map(|(alias, _)| alias);
        let (text, parent) = instantiate(registry, concerns, record, &arguments)?;
        let parent =
            parent.and_then(|raw| lex::tokens(&raw).first().and_then(|first| first.name(&raw)));
        edits.push((token.range.start..end, text));
        placed.push(Placed { alias, record: record.clone(), parent });
        index = next;
    }
    Ok(Expansion { sql: lex::apply(sql, &edits), placed })
}

fn arguments_of(
    sql: &str,
    tokens: &[Token],
    range: Range<usize>,
    concerns: &str,
    fragment: &str,
) -> Result<Vec<Argument>, Cause> {
    let bad = || Cause::BadSpread { concerns: concerns.to_owned(), fragment: fragment.to_owned() };
    let mut arguments = Vec::new();
    let mut depth = 0usize;
    let mut start = range.start;
    for index in range.start..=range.end {
        let at_end = index == range.end;
        if !at_end {
            let token = &tokens[index];
            if token.is_symbol(sql, '(') {
                depth += 1;
            } else if token.is_symbol(sql, ')') {
                depth = depth.saturating_sub(1);
            }
        }
        if at_end || (depth == 0 && tokens[index].is_symbol(sql, ',')) {
            if start == index {
                if at_end && start == range.start {
                    break;
                }
                return Err(bad());
            }
            let parts = &tokens[start..index];
            let name = parts[0].name(sql).ok_or_else(bad)?;
            if parts.len() < 3 || !parts[1].is_symbol(sql, ':') {
                return Err(bad());
            }
            let value = &parts[2..];
            let single = (value.len() == 1).then(|| value[0].clone());
            arguments.push(Argument {
                name,
                value: sql[value[0].range.start..value[value.len() - 1].range.end].to_owned(),
                single_text: single.as_ref().map(|token| token.text(sql).to_owned()),
                single,
            });
            start = index + 1;
        }
    }
    Ok(arguments)
}

/// The SQL of `record` as a subquery for the spread whose arguments are
/// `arguments`, and the alias that gives its parent row.
fn instantiate(
    registry: &Registry,
    concerns: &str,
    record: &Record,
    arguments: &[Argument],
) -> Result<(String, Option<String>), Cause> {
    let fragment = record.path.clone();
    for (position, argument) in arguments.iter().enumerate() {
        let known = argument.name == "parent"
            || record.parameters.iter().any(|parameter| parameter.name == argument.name)
            || record.relations.iter().any(|relation| relation.name == argument.name);
        if !known {
            return Err(Cause::UnknownArgument { fragment, argument: argument.name.clone() });
        }
        if arguments[..position].iter().any(|earlier| earlier.name == argument.name) {
            return Err(Cause::DuplicateArgument { fragment, argument: argument.name.clone() });
        }
    }
    let find = |name: &str| arguments.iter().find(|argument| argument.name == name);
    let parent = match (&record.parent, find("parent")) {
        (Some(_), Some(argument)) => {
            let alias = argument.single.as_ref().is_some_and(|token| token.kind != Kind::Parameter);
            if !alias {
                return Err(Cause::BadArgument { fragment, argument: "parent".to_owned() });
            }
            argument.single_text.clone()
        }
        (Some(_), None) => return Err(Cause::MissingParent { fragment }),
        (None, Some(_)) => return Err(Cause::NoParentRow { fragment }),
        (None, None) => None,
    };
    let mut bindings = BTreeMap::new();
    for parameter in &record.parameters {
        let argument = find(&parameter.name).ok_or_else(|| Cause::NoValue {
            fragment: fragment.clone(),
            parameter: parameter.name.clone(),
        })?;
        match (&argument.single, &argument.single_text) {
            (Some(token), Some(text)) if token.kind == Kind::Parameter && text.starts_with(':') => {
                bindings.insert(parameter.name.clone(), text.clone());
            }
            _ => {
                return Err(Cause::BadArgument { fragment, argument: parameter.name.clone() });
            }
        }
    }
    let mut ctes = Vec::new();
    for relation in &record.relations {
        let argument = find(&relation.name).ok_or_else(|| Cause::MissingRelation {
            fragment: fragment.clone(),
            relation: relation.name.clone(),
        })?;
        let rows = expand(registry, concerns, false, &argument.value)?.sql;
        if !rows.trim_start().starts_with('(') {
            return Err(Cause::BadArgument { fragment, argument: relation.name.clone() });
        }
        let columns: Vec<&str> = relation.columns.iter().map(|(name, _)| name.as_str()).collect();
        ctes.push(format!(
            "{} ({}) AS {rows}",
            renamed(&record_prefix(&record.path), &relation.name),
            columns.join(", ")
        ));
    }
    let body = substitute(&record.sql, &bindings, parent.as_deref());
    Ok((format!("({})", with_ctes(&ctes, &body)), parent))
}

/// The prefix of the CTE names of the fragment at `path`: its crate and its
/// name.
pub(crate) fn record_prefix(path: &str) -> String {
    let crate_name = path.split("::").next().unwrap_or(path);
    let name = path.rsplit("::").next().unwrap_or(path);
    format!("{crate_name}__{name}")
}

/// `sql` with each parameter that `bindings` names replaced by what it binds,
/// and each `@parent.column` by that column of the FROM item `parent`.
fn substitute(sql: &str, bindings: &BTreeMap<String, String>, parent: Option<&str>) -> String {
    let tokens = lex::tokens(sql);
    let mut edits = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let token = &tokens[index];
        match token.kind {
            Kind::Parameter => {
                if let Some(bound) = bindings.get(&token.text(sql)[1..]) {
                    edits.push((token.range.clone(), bound.clone()));
                }
            }
            Kind::Marker if token.text(sql) == "@parent" => {
                if let (Some(alias), Some(column)) = (parent, tokens.get(index + 2)) {
                    edits.push((
                        token.range.start..column.range.end,
                        format!("{alias}.{}", column.text(sql)),
                    ));
                    index += 2;
                }
            }
            _ => {}
        }
        index += 1;
    }
    lex::apply(sql, &edits)
}

/// `body` under the CTEs `ctes`, which come before the CTEs it declares.
pub(crate) fn with_ctes(ctes: &[String], body: &str) -> String {
    if ctes.is_empty() {
        return body.trim().to_owned();
    }
    let tokens = lex::tokens(body);
    let declared = tokens.first().is_some_and(|first| first.is_word(body, "with"));
    if !declared {
        return format!("WITH {} {}", ctes.join(", "), body.trim());
    }
    let recursive = tokens.get(1).is_some_and(|next| next.is_word(body, "recursive"));
    let rest_at =
        tokens.get(if recursive { 2 } else { 1 }).map_or(body.len(), |token| token.range.start);
    let keyword = if recursive { "WITH RECURSIVE" } else { "WITH" };
    format!("{keyword} {}, {}", ctes.join(", "), body[rest_at..].trim())
}

/// `sql` of a fragment that reads a parent row of `table`, with each
/// `@parent.column` a subquery that reads that column of the table, so that
/// SQLite can prepare it with no row to read.
pub(crate) fn without_parent(sql: &str, table: &str) -> String {
    let tokens = lex::tokens(sql);
    let mut edits = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let token = &tokens[index];
        if token.kind == Kind::Marker && token.text(sql) == "@parent" {
            if let Some(column) = tokens.get(index + 2) {
                edits.push((
                    token.range.start..column.range.end,
                    format!("(SELECT {} FROM {table} WHERE 0)", column.text(sql)),
                ));
                index += 2;
            }
        }
        index += 1;
    }
    lex::apply(sql, &edits)
}

/// The CTE that stands for the relation `relation` of a fragment, with no
/// rows, in the columns the header gives it.
pub(crate) fn empty_relation(prefix: &str, relation: &super::header::Relation) -> String {
    let columns: Vec<&str> = relation.columns.iter().map(|(name, _)| name.as_str()).collect();
    let nulls: Vec<String> =
        relation.columns.iter().map(|(_, ty)| format!("CAST(NULL AS {ty})")).collect();
    format!(
        "{} ({}) AS (SELECT {} WHERE 0)",
        renamed(prefix, &relation.name),
        columns.join(", "),
        nulls.join(", ")
    )
}

#[cfg(test)]
#[path = "spread_tests.rs"]
mod tests;
