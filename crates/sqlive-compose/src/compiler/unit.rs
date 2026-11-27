//! The compile of one macro call: its spreads expanded, its SQL checked
//! against the schema, its columns typed, and its Rust written.

use std::collections::BTreeSet;
use std::ops::Range;

use sqlive_reads::{ReadPlace, SqlPosition};

use super::columns;
use super::core::{self, all_from_items};
use super::error::{Cause, CompileError, Warning, WholePlace};
use super::generate;
use super::header::{Header, HeaderKind};
use super::lex::{self, Origin};
use super::model::{Field, Reading, Record};
use super::rename;
use super::source::Call;
use super::spread::{self, Expansion, Place, Placed, Registry};
use super::write;

/// What compiling a call needs of the crate it is in.
pub(crate) struct Context<'a> {
    pub(crate) crate_name: &'a str,
    pub(crate) probe: &'a sqlive_reads::SchemaProbe,
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
    /// The tables and views the call's own SQL reads.
    pub(crate) reads: BTreeSet<String>,
}

/// Where a call whose SQL is `written` reads a table or view whole: `place`
/// is where the SQL the read runs names it, which `renames` and then
/// `expansion`'s spreads made of `written`.
fn whole_place(
    written: &str,
    renames: &[(Range<usize>, String)],
    expansion: &Expansion,
    place: ReadPlace,
) -> WholePlace {
    // A rename replaces a CTE's or relation's name with text that starts
    // where the name does.
    let unrenamed = |offset| match lex::origin(renames, offset) {
        Origin::Kept(offset) => offset,
        Origin::Edit(range) => range.start,
    };
    let position = |offset| {
        let SqlPosition { line, column, .. } = SqlPosition::at(written, offset);
        (line, column)
    };
    match place {
        ReadPlace::Within(name) => WholePlace::Within(name),
        ReadPlace::Statement(run) => match lex::origin(&expansion.edits, run.offset) {
            Origin::Kept(offset) => {
                let (line, column) = position(unrenamed(offset));
                WholePlace::Sql { line, column }
            }
            Origin::Edit(spread) => {
                let at = unrenamed(spread.start);
                let marker = lex::tokens(&written[at..])[0].text(&written[at..]).to_owned();
                let (line, column) = position(at);
                WholePlace::Spread { marker, line, column }
            }
        },
    }
}

/// Compile `call`, whose header is `header`.
pub(crate) fn compile(
    context: &Context<'_>,
    call: &Call,
    header: &Header,
) -> Result<Compiled, CompileError> {
    let fail = |cause: Cause| CompileError::Call {
        location: call.location.clone(),
        concerns: header.name.clone(),
        cause,
    };
    if header.is_write() {
        return compile_write(context, call, header);
    }
    let crate_path = context.crate_name.replace('-', "_");
    let prefix = format!("{crate_path}__{}", header.name);
    let relation_names: Vec<String> =
        header.relations().iter().map(|relation| relation.name.clone()).collect();
    let renames = rename::edits(&header.sql, &prefix, &relation_names);
    let renamed = lex::apply(&header.sql, &renames);
    let reads = own_reads(header);
    let expansion =
        spread::expand(context.registry, &header.name, header.parent().is_some(), &renamed)
            .map_err(&fail)?;
    let sql = expansion.sql.as_str();
    let standing: Vec<String> = header
        .relations()
        .iter()
        .map(|relation| spread::empty_relation(&prefix, relation))
        .collect();
    let prepared_sql = match header.parent() {
        Some(table) => spread::with_ctes(&standing, &spread::without_parent(sql, table)),
        None => spread::with_ctes(&standing, sql),
    };
    let tokens = lex::tokens(&prepared_sql);
    check_spreads(&expansion.placed, &all_from_items(&prepared_sql, &tokens)).map_err(&fail)?;
    let prepared =
        context.probe.prepare(&prepared_sql).map_err(|error| fail(Cause::Sqlite(error)))?;
    if prepared.writes {
        return Err(fail(Cause::WriteInRead { concerns: header.name.clone() }));
    }
    check_parameters(header, &prepared).map_err(&fail)?;
    let mut warnings = Vec::new();
    // A read ships its scope plan, built from the SQL it runs; a fragment
    // runs only inside the reads that spread it.
    let (report, plan) = if header.is_read() {
        let built = context.probe.compiled_plan(sql).map_err(|error| fail(Cause::Sqlite(error)))?;
        let plan = built.map_err(|sqlive_reads::Unanalyzable { why }| {
            fail(Cause::Unanalyzable { concerns: header.name.clone(), why })
        })?;
        (plan.report(), Some(plan.encode()))
    } else {
        let report =
            context.probe.read_report(&prepared_sql).map_err(|error| fail(Cause::Sqlite(error)))?;
        (report, None)
    };
    match report {
        sqlive_reads::ReadReport::Whole { why } => {
            return Err(fail(Cause::Unanalyzable { concerns: header.name.clone(), why }));
        }
        sqlive_reads::ReadReport::Entries(entries) if header.is_read() => {
            for entry in entries {
                if let sqlive_reads::ReadRows::Whole { why } = entry.rows {
                    let place = entry.place.expect("a compiled plan's report has each place");
                    warnings.push(Warning {
                        location: call.location.clone(),
                        concerns: header.name.clone(),
                        table: entry.object,
                        place: whole_place(&header.sql, &renames, &expansion, place),
                        reason: why.join(", "),
                    });
                }
            }
        }
        sqlive_reads::ReadReport::Entries(_) => {}
    }
    let outline = core::outline(&prepared_sql, &tokens);
    if header.parent().is_some() && prepared.columns.len() != 1 {
        return Err(fail(Cause::ParentColumns {
            concerns: header.name.clone(),
            width: prepared.columns.len(),
        }));
    }
    let fields = columns::fields(
        header,
        &outline,
        &prepared,
        &expansion.placed,
        context.registry,
        &prepared_sql,
        &tokens,
    )
    .map_err(&fail)?;
    let (code, record) = match &header.kind {
        HeaderKind::Fragment { parent, relations } => {
            let record = Record {
                path: fragment_path(context.crate_name, &call.module, &header.name),
                parameters: header.parameters.clone(),
                parent: parent.clone(),
                relations: relations.clone(),
                sql: sql.to_owned(),
                width: prepared.columns.len(),
                required: required(&fields),
                one_row: outline.one_row(),
            };
            (generate::fragment(header, &fields, call.exported), Some(record))
        }
        HeaderKind::Read => {
            let plan = plan.as_deref().expect("a read's plan is built before its columns");
            (generate::read(header, sql, plan, &fields, call.exported), None)
        }
        HeaderKind::Write => unreachable!("a write compiles in compile_write"),
    };
    Ok(Compiled { code, record, warnings, reads })
}

/// Compile the write `call`, whose header is `header`. It has no relations
/// and no parent row, and sqlive analyzes none of it, since no live query
/// runs a write.
fn compile_write(
    context: &Context<'_>,
    call: &Call,
    header: &Header,
) -> Result<Compiled, CompileError> {
    let fail = |cause: Cause| CompileError::Call {
        location: call.location.clone(),
        concerns: header.name.clone(),
        cause,
    };
    let crate_path = context.crate_name.replace('-', "_");
    let prefix = format!("{crate_path}__{}", header.name);
    let renamed = rename::rename(&header.sql, &prefix, &[]);
    let expansion =
        spread::expand(context.registry, &header.name, false, &renamed).map_err(&fail)?;
    let sql = expansion.sql.as_str();
    let tokens = lex::tokens(sql);
    let target = write::target(sql, &tokens)
        .ok_or_else(|| fail(Cause::NotAWrite { concerns: header.name.clone() }))?;
    // A spread that reads a parent row may name the table the write changes.
    let mut from = all_from_items(sql, &tokens);
    from.push(target.item.clone());
    check_spreads(&expansion.placed, &from).map_err(&fail)?;
    let prepared = context.probe.prepare(sql).map_err(|error| fail(Cause::Sqlite(error)))?;
    check_parameters(header, &prepared).map_err(&fail)?;
    let table_columns =
        context.probe.table_columns(&target.table).map_err(|error| fail(Cause::Sqlite(error)))?;
    let outline = core::outline(sql, &tokens);
    write::check_not_null(header, sql, &tokens, &target, &table_columns, &outline.items)
        .map_err(&fail)?;
    let returning = write::returning(sql, &tokens, target.item);
    let fields =
        columns::fields(header, &returning, &prepared, &[], context.registry, sql, &tokens)
            .map_err(&fail)?;
    Ok(Compiled {
        code: generate::write(header, sql, &fields),
        record: None,
        warnings: Vec::new(),
        reads: own_reads(header),
    })
}

/// The tables and views that `header`'s own SQL reads by name in FROM and
/// JOIN, at any depth. A CTE it declares, a relation it is given and a spread
/// are not among them: a spread's fragment's own SQL reads what it reads.
fn own_reads(header: &Header) -> BTreeSet<String> {
    let tokens = lex::tokens(&header.sql);
    let relations: Vec<String> =
        header.relations().iter().map(|relation| relation.name.clone()).collect();
    let own = rename::renamed_names(&header.sql, &relations);
    core::all_from_items(&header.sql, &tokens)
        .iter()
        .filter_map(core::FromItem::table)
        .filter(|name| !own.contains(*name))
        .map(str::to_owned)
        .collect()
}

/// The positions, among a fragment's columns, of those that each of its rows
/// holds a value in.
fn required(fields: &[Field]) -> Vec<usize> {
    let mut required = Vec::new();
    for field in fields {
        match &field.reading {
            _ if field.ty.is_option() => {}
            Reading::Value | Reading::Json => required.push(field.column),
            Reading::Spread { required: inner, .. } => {
                required.extend(inner.iter().map(|inner| field.column + inner));
            }
            Reading::MissingSpread { .. } => {}
        }
    }
    required
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
        let Place::Value { parent, table } = &spread.place else {
            continue;
        };
        if let Some(alias) = &spread.alias {
            let in_from = from.iter().any(|item| {
                item.table().is_none() && item.qualifier.as_deref() == Some(alias.as_str())
            });
            if in_from {
                return Err(Cause::ParentSpreadInFrom { fragment: spread.record.path.clone() });
            }
        }
        let reads = from.iter().any(|item| {
            item.qualifier.as_deref() == Some(parent.as_str())
                && item.table().is_some_and(|read| read.eq_ignore_ascii_case(table))
        });
        if !reads {
            return Err(Cause::WrongParentTable {
                fragment: spread.record.path.clone(),
                alias: parent.clone(),
                table: table.clone(),
            });
        }
    }
    Ok(())
}

/// Whether the parameters the statement reads are the ones the header
/// declares, each written `:name`.
fn check_parameters(
    header: &Header,
    prepared: &sqlive_reads::ProbedStatement,
) -> Result<(), Cause> {
    if prepared.unnamed_parameters > 0 {
        return Err(Cause::ForeignParameter {
            concerns: header.name.clone(),
            parameter: "?".to_owned(),
        });
    }
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

#[cfg(test)]
#[path = "unit_tests.rs"]
mod tests;
