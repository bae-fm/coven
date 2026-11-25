//! The compile of one macro call: its spreads expanded, its SQL checked
//! against the schema, its columns typed, and its Rust written.

use std::collections::BTreeSet;

use super::columns;
use super::core::{self, all_from_items};
use super::error::{Cause, CompileError, Warning};
use super::generate;
use super::header::{CallKind, Header};
use super::lex;
use super::model::{Field, Reading, Record};
use super::rename;
use super::source::Call;
use super::spread::{self, Placed, Registry};

/// What compiling a call needs of the crate it is in.
pub(crate) struct Context<'a> {
    pub(crate) crate_name: &'a str,
    pub(crate) probe: &'a sqlive::SchemaProbe,
    pub(crate) registry: &'a Registry,
}

/// What a call compiles to.
pub(crate) struct Compiled {
    /// The Rust the call's macro includes.
    pub(crate) code: String,
    /// The fragment's record, for the crates that spread it.
    pub(crate) record: Option<Record>,
    /// The read's warnings.
    pub(crate) warnings: Vec<Warning>,
}

/// Compile `call`, whose header is `header`.
pub(crate) fn compile(
    context: &Context<'_>,
    call: &Call,
    header: &Header,
) -> Result<Compiled, CompileError> {
    let fail = |cause: Cause| CompileError {
        location: Some(call.location.clone()),
        concerns: header.name.clone(),
        cause,
    };
    let crate_path = context.crate_name.replace('-', "_");
    let prefix = format!("{crate_path}__{}", header.name);
    let relation_names: Vec<String> =
        header.relations.iter().map(|relation| relation.name.clone()).collect();
    let renamed = rename::rename(&header.sql, &prefix, &relation_names);
    let expansion =
        spread::expand(context.registry, &header.name, header.parent.is_some(), &renamed)
            .map_err(&fail)?;
    let sql = expansion.sql.as_str();
    let standing: Vec<String> =
        header.relations.iter().map(|relation| spread::empty_relation(&prefix, relation)).collect();
    let prepared_sql = match &header.parent {
        Some(table) => spread::with_ctes(&standing, &spread::without_parent(sql, table)),
        None => spread::with_ctes(&standing, sql),
    };
    let tokens = lex::tokens(&prepared_sql);
    check_spreads(&expansion.placed, &all_from_items(&prepared_sql, &tokens)).map_err(&fail)?;
    let prepared =
        context.probe.prepare(&prepared_sql).map_err(|error| fail(Cause::Sqlite(error)))?;
    check_parameters(header, &prepared).map_err(&fail)?;
    let outline = core::outline(&prepared_sql, &tokens);
    if header.parent.is_some() && prepared.columns.len() != 1 {
        return Err(fail(Cause::ParentColumns {
            concerns: header.name.clone(),
            width: prepared.columns.len(),
        }));
    }
    let fields = columns::fields(header, &outline, &prepared, &expansion.placed, context.registry)
        .map_err(&fail)?;
    let mut warnings = Vec::new();
    match context.probe.read_report(&prepared_sql).map_err(|error| fail(Cause::Sqlite(error)))? {
        sqlive::ReadReport::Whole { why } => {
            return Err(fail(Cause::Unanalyzable { concerns: header.name.clone(), why }));
        }
        sqlive::ReadReport::Entries(entries) if header.kind == CallKind::Read => {
            for entry in entries {
                if let sqlive::ReadRows::Whole { why } = entry.rows {
                    warnings.push(Warning {
                        location: call.location.clone(),
                        concerns: header.name.clone(),
                        table: entry.object,
                        reason: why.join(", "),
                    });
                }
            }
        }
        sqlive::ReadReport::Entries(_) => {}
    }
    let (code, record) = match header.kind {
        CallKind::Fragment => {
            let record = Record {
                path: fragment_path(context.crate_name, &call.module, &header.name),
                parameters: header.parameters.clone(),
                parent: header.parent.clone(),
                relations: header.relations.clone(),
                sql: sql.to_owned(),
                width: prepared.columns.len(),
                presence: presence(&fields),
                one_row: outline.aggregates && !outline.groups,
            };
            (generate::fragment(header, &fields), Some(record))
        }
        CallKind::Read => (generate::read(header, sql, &fields), None),
    };
    Ok(Compiled { code, record, warnings })
}

/// The position, among a fragment's columns, of the first one that can't be
/// NULL.
fn presence(fields: &[Field]) -> Option<usize> {
    fields.iter().find_map(|field| match &field.reading {
        _ if field.ty.is_option() => None,
        Reading::Value | Reading::Json => Some(field.column),
        Reading::Spread { presence, .. } => presence.map(|inner| field.column + inner),
    })
}

/// The Rust path of a fragment type: its crate's name, the modules it is in
/// and its own name.
pub(crate) fn fragment_path(crate_name: &str, module: &[String], name: &str) -> String {
    let mut path = crate_name.replace('-', "_");
    for part in module {
        path.push_str("::");
        path.push_str(part);
    }
    format!("{path}::{name}")
}

/// Whether each spread sits where its fragment can: a parent row's value
/// beside a FROM item that reads the parent's table, never in FROM itself.
fn check_spreads(placed: &[Placed], from: &[core::FromItem]) -> Result<(), Cause> {
    for spread in placed {
        let Some(table) = &spread.record.parent else { continue };
        if let Some(alias) = &spread.alias {
            let in_from = from.iter().any(|item| {
                item.table.is_none() && item.qualifier.as_deref() == Some(alias.as_str())
            });
            if in_from {
                return Err(Cause::ParentSpreadInFrom { fragment: spread.record.path.clone() });
            }
        }
        let parent = spread.parent.clone().unwrap_or_default();
        let reads = from.iter().any(|item| {
            item.qualifier.as_deref() == Some(parent.as_str())
                && item.table.as_deref().is_some_and(|read| read.eq_ignore_ascii_case(table))
        });
        if !reads {
            return Err(Cause::WrongParentTable {
                fragment: spread.record.path.clone(),
                alias: parent,
                table: table.clone(),
            });
        }
    }
    Ok(())
}

/// Whether the parameters the statement reads are the ones the header
/// declares, each written `:name`.
fn check_parameters(header: &Header, prepared: &sqlive::ProbedStatement) -> Result<(), Cause> {
    let mut read = BTreeSet::new();
    for parameter in &prepared.parameters {
        let Some(name) = parameter.strip_prefix(':') else {
            return Err(Cause::ForeignParameter {
                concerns: header.name.clone(),
                parameter: parameter.clone(),
            });
        };
        if !header.parameters.iter().any(|declared| declared.name == name) {
            return Err(Cause::UndeclaredParameter {
                concerns: header.name.clone(),
                parameter: name.to_owned(),
            });
        }
        read.insert(name);
    }
    for declared in &header.parameters {
        if !read.contains(declared.name.as_str()) {
            return Err(Cause::UnreadParameter {
                concerns: header.name.clone(),
                parameter: declared.name.clone(),
            });
        }
    }
    Ok(())
}
