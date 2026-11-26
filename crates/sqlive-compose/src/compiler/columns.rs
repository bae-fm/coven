//! The typed fields of a call's statement: where each result column comes
//! from in a table or in a spread, whether it can be NULL, and the Rust type
//! that reads it.

use sqlive_reads::{ColumnNulls, SelectNull};

use super::core::{Core, Item, ItemKind};
use super::error::Cause;
use super::header::{Declared, Header, RustType};
use super::json;
use super::lex::Token;
use super::model::{strict_type, Field, FieldName, Reading};
use super::never_null;
use super::spread::{Place, Placed, Registry};

/// A result item and the result columns it gives.
struct Span<'a> {
    item: &'a Item,
    start: usize,
    width: usize,
    /// The fragment it spreads, when it is `alias.*` of a table spread or a
    /// value spread's alias.
    spread: Option<(&'a Placed, &'a str)>,
}

/// Each field of the call's type, in the order of the result items.
pub(crate) fn fields(
    header: &Header,
    core: &Core,
    prepared: &sqlive_reads::ProbedStatement,
    placed: &[Placed],
    registry: &Registry,
    sql: &str,
    tokens: &[Token],
) -> Result<Vec<Field>, Cause> {
    let spans = spans(header, core, prepared.columns.len(), placed)?;
    let mut fields: Vec<Field> = Vec::new();
    for span in &spans {
        if let Some((spread, alias)) = span.spread {
            let columns = &prepared.columns[span.start..span.start + span.width];
            let field = spread_field(header, columns, span, (spread, alias), registry)?;
            push(header, &mut fields, field)?;
            continue;
        }
        if let ItemKind::Column { qualifier: Some(qualifier), name } = &span.item.kind {
            if placed.iter().any(|spread| spread.alias.as_deref() == Some(qualifier.as_str())) {
                return Err(Cause::SpreadColumn {
                    concerns: header.name.clone(),
                    column: format!("{qualifier}.{name}"),
                });
            }
        }
        let columns = prepared.columns.iter().enumerate().skip(span.start).take(span.width);
        for (position, column) in columns {
            let field = column_field(header, span.item, column, position, (sql, tokens))?;
            push(header, &mut fields, field)?;
        }
    }
    for declaration in &header.declarations {
        if !prepared.columns.iter().any(|column| column.name == declaration.column) {
            return Err(Cause::UnreturnedColumn {
                concerns: header.name.clone(),
                column: declaration.column.clone(),
            });
        }
    }
    Ok(fields)
}

fn push(header: &Header, fields: &mut Vec<Field>, field: Field) -> Result<(), Cause> {
    if fields.iter().any(|other| other.name == field.name) {
        return Err(Cause::DuplicateColumn {
            concerns: header.name.clone(),
            column: field.name.as_text(),
        });
    }
    fields.push(field);
    Ok(())
}

/// The result items with the columns each gives. A `*` takes the columns
/// that the other items leave; a compound statement has no item to align.
fn spans<'a>(
    header: &Header,
    core: &'a Core,
    columns: usize,
    placed: &'a [Placed],
) -> Result<Vec<Span<'a>>, Cause> {
    let spread_of = |item: &Item| -> Option<(&'a Placed, &'a str)> {
        placed.iter().find_map(|spread| {
            let alias = spread.alias.as_deref()?;
            let named = match (&item.kind, &spread.place) {
                (ItemKind::QualifiedStar(qualifier), Place::Table) => qualifier == alias,
                (ItemKind::Subquery, Place::Value { .. }) => item.alias.as_deref() == Some(alias),
                _ => false,
            };
            named.then_some((spread, alias))
        })
    };
    let width_of = |item: &Item| match (spread_of(item), &item.kind) {
        (Some((spread, _)), ItemKind::QualifiedStar(_)) => Some(spread.record.width),
        (_, ItemKind::Star | ItemKind::QualifiedStar(_)) => None,
        _ => Some(1),
    };
    let stars = core.items.iter().filter(|item| width_of(item).is_none()).count();
    if stars > 1 {
        return Err(Cause::Stars { concerns: header.name.clone() });
    }
    let known: usize = core.items.iter().filter_map(width_of).sum();
    let star_width = columns.saturating_sub(known);
    let mut spans = Vec::new();
    let mut start = 0;
    for item in &core.items {
        let width = width_of(item).unwrap_or(star_width);
        spans.push(Span { item, start, width, spread: spread_of(item) });
        start += width;
    }
    if start != columns {
        return Err(Cause::Unaligned { concerns: header.name.clone() });
    }
    Ok(spans)
}

/// The field for the columns of a spread.
fn spread_field(
    header: &Header,
    columns: &[sqlive_reads::ProbedColumn],
    span: &Span<'_>,
    (spread, alias): (&Placed, &str),
    registry: &Registry,
) -> Result<Field, Cause> {
    // Of a statement the probe can't read, any column can be NULL.
    let read: Option<Vec<(&str, &ColumnNulls)>> =
        columns.iter().map(|column| Some((column.name.as_str(), column.nulls.as_ref()?))).collect();
    if let Some(read) = &read {
        check_later_selects(header, read, (spread, alias))?;
    }
    let can_be_missing = match &spread.place {
        Place::Value { .. } => !spread.record.one_row,
        // The columns of `alias.*` come from one FROM item, the spread's.
        Place::Table => {
            read.as_ref().is_none_or(|read| read.iter().any(|(_, nulls)| nulls.row_can_be_missing))
        }
    };
    // A later SELECT of a compound gives no row of the fragment where it can
    // give NULL in the column that each of the fragment's rows holds.
    let later_can_give_none = spread.record.presence().is_some_and(|presence| {
        read.as_ref().is_none_or(|read| {
            read[presence].1.selects[1..].iter().any(|select| *select != SelectNull::Never)
        })
    });
    let path = registry.code_path(&spread.record.path);
    let (ty, reading) = if can_be_missing || later_can_give_none {
        let presence = spread.record.presence().ok_or_else(|| Cause::NoPresence {
            concerns: header.name.clone(),
            fragment: spread.record.path.clone(),
        })?;
        (format!("Option<{path}>"), Reading::MissingSpread { path, presence })
    } else {
        let required = spread.record.required.clone();
        (path.clone(), Reading::Spread { path, required })
    };
    Ok(Field { name: field_name(header, alias)?, ty: RustType(ty), reading, column: span.start })
}

/// Whether each later SELECT of a compound gives the spread, whose columns
/// are `columns`, each with its name, a row of its fragment or none: it fills each column that
/// each row of the fragment holds, or gives NULL in such a column only where
/// it gives NULL in the one that tells a missing row too.
fn check_later_selects(
    header: &Header,
    columns: &[(&str, &ColumnNulls)],
    (spread, alias): (&Placed, &str),
) -> Result<(), Cause> {
    let Some((&presence, others)) = spread.record.required.split_first() else {
        return Ok(());
    };
    for (select, &tells) in columns[presence].1.selects.iter().enumerate().skip(1) {
        for &other in others {
            let held = match columns[other].1.selects[select] {
                SelectNull::Never | SelectNull::Computed => true,
                // The reader reads no row where every row of the SELECT
                // gives NULL in the column that tells one.
                _ if tells == SelectNull::Literal => true,
                SelectNull::MissingRow(item) => tells == SelectNull::MissingRow(item),
                SelectNull::Literal | SelectNull::Value => false,
            };
            if !held {
                return Err(Cause::NullInSpread {
                    concerns: header.name.clone(),
                    select: select + 1,
                    column: format!("{alias}.{}", columns[other].0),
                    fragment: spread.record.path.clone(),
                });
            }
        }
    }
    Ok(())
}

/// The first SELECT of a compound, counted from 1, that can give NULL in the
/// column of `nulls`: all but a computed value, whose header type says whether
/// it can be.
fn null_select(nulls: &ColumnNulls) -> Option<usize> {
    nulls
        .selects
        .iter()
        .position(|select| !matches!(select, SelectNull::Never | SelectNull::Computed))
        .map(|select| select + 1)
}

/// The field for the result column `column`, which comes from `item`.
fn column_field(
    header: &Header,
    item: &Item,
    column: &sqlive_reads::ProbedColumn,
    position: usize,
    (sql, tokens): (&str, &[Token]),
) -> Result<Field, Cause> {
    let declared = header.declarations.iter().find(|declaration| declaration.column == column.name);
    let compound = column.nulls.as_ref().filter(|nulls| nulls.selects.len() > 1);
    // SQLite names the origin of a compound's first SELECT only.
    let origin = if compound.is_some() { None } else { column.origin.as_ref() };
    let null_select = compound.and_then(null_select);
    let nullable = match &column.nulls {
        // Of a statement the probe can't read, any column can be NULL.
        None => true,
        Some(_) if compound.is_some() => null_select.is_some(),
        Some(_) if item.kind == ItemKind::Subquery => {
            !never_null::expression(sql, &tokens[item.expression.clone()])
        }
        Some(nulls) => origin.is_some() && nulls.nullable(),
    };
    let (ty, reading) = match (declared.map(|declaration| &declaration.declared), origin) {
        (Some(Declared::Json(ty)), _) => {
            if nullable && !ty.is_option() {
                return Err(Cause::NullableColumn {
                    column: column.name.clone(),
                    select: null_select,
                    declared: format!("json<{}>", ty.0),
                });
            }
            if matches!(item.kind, ItemKind::Expression | ItemKind::Subquery) {
                json::check(sql, tokens, item.tokens.clone(), &column.name, ty, &header.shapes)?;
            }
            (ty.clone(), Reading::Json)
        }
        (Some(Declared::Type(ty)), _) => {
            if nullable && !ty.is_option() {
                return Err(Cause::NullableColumn {
                    column: column.name.clone(),
                    select: null_select,
                    declared: ty.0.clone(),
                });
            }
            (ty.clone(), Reading::Value)
        }
        (None, Some(origin)) => {
            let mapped =
                origin.declared_type.as_deref().and_then(strict_type).ok_or_else(|| {
                    Cause::UnmappedType {
                        concerns: header.name.clone(),
                        column: column.name.clone(),
                        declared: origin
                            .declared_type
                            .clone()
                            .unwrap_or_else(|| "no type".to_owned()),
                    }
                })?;
            let ty = if nullable { format!("Option<{mapped}>") } else { mapped.to_owned() };
            (RustType(ty), Reading::Value)
        }
        (None, None) => {
            return Err(Cause::UntypedColumn {
                concerns: header.name.clone(),
                column: column.name.clone(),
            })
        }
    };
    let name = field_name(header, &column.name)?;
    Ok(Field { name, ty, reading, column: position })
}

/// The name of the field for the column or spread `name`.
fn field_name(header: &Header, name: &str) -> Result<FieldName, Cause> {
    FieldName::new(name).ok_or_else(|| Cause::BadIdentifier {
        concerns: header.name.clone(),
        name: name.to_owned(),
    })
}

#[cfg(test)]
#[path = "columns_tests.rs"]
mod tests;
