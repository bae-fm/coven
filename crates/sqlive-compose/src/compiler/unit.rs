//! The compile of one macro call: its SQL checked against the schema, its
//! columns typed, and its Rust written.

use std::collections::BTreeSet;

use super::columns;
use super::core;
use super::error::{Cause, CompileError, Warning};
use super::generate;
use super::header::{CallKind, Header};
use super::lex::{self, Kind};
use super::model::Record;
use super::source::Call;

/// What compiling a call needs of the crate it is in.
pub(crate) struct Context<'a> {
    pub(crate) crate_name: &'a str,
    pub(crate) probe: &'a sqlive::SchemaProbe,
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
    let sql = header.sql.as_str();
    let tokens = lex::tokens(sql);
    if let Some(marker) = tokens.iter().find(|token| token.kind == Kind::Marker) {
        return Err(fail(Cause::StrayMarker {
            concerns: header.name.clone(),
            text: marker.text(sql).to_owned(),
        }));
    }
    let prepared = context.probe.prepare(sql).map_err(|error| fail(Cause::Sqlite(error)))?;
    check_parameters(header, &prepared).map_err(&fail)?;
    let outline = core::outline(sql, &tokens);
    let fields = columns::fields(header, &outline, &prepared).map_err(&fail)?;
    let mut warnings = Vec::new();
    match context.probe.read_report(sql).map_err(|error| fail(Cause::Sqlite(error)))? {
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
            let presence = fields.iter().position(|field| !field.ty.is_option());
            let record = Record {
                path: fragment_path(context.crate_name, &call.module, &header.name),
                parameters: header.parameters.clone(),
                parent: header.parent.clone(),
                relations: header.relations.clone(),
                sql: header.sql.clone(),
                width: fields.len(),
                presence,
                one_row: outline.aggregates && !outline.groups,
            };
            (generate::fragment(header, &fields), Some(record))
        }
        CallKind::Read => (generate::read(header, sql, &fields), None),
    };
    Ok(Compiled { code, record, warnings })
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
