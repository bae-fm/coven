//! The typed fields of a call's statement: where each result column comes
//! from in a table or in a spread, whether it can be NULL, and the Rust type
//! that reads it.

use super::core::{Core, FromItem, Item, ItemKind, Join};
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
    let nullable_from = nullable_items(core);
    let mut fields: Vec<Field> = Vec::new();
    for span in &spans {
        if let Some((spread, alias)) = span.spread {
            let field =
                spread_field(header, core, &nullable_from, span, (spread, alias), registry)?;
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
            let field = column_field(
                header,
                core,
                &nullable_from,
                span.item,
                column,
                position,
                (sql, tokens),
            )?;
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
    core: &Core,
    nullable_from: &[bool],
    span: &Span<'_>,
    (spread, alias): (&Placed, &str),
    registry: &Registry,
) -> Result<Field, Cause> {
    let nullable = match &spread.place {
        Place::Value { .. } => !spread.record.one_row,
        Place::Table => core
            .from
            .iter()
            .zip(nullable_from)
            .any(|(from, &nullable)| nullable && from.qualifier.as_deref() == Some(alias)),
    };
    let path = registry.code_path(&spread.record.path);
    let (ty, reading) = if nullable {
        let presence = spread.record.presence.ok_or_else(|| Cause::NoPresence {
            concerns: header.name.clone(),
            fragment: spread.record.path.clone(),
        })?;
        (format!("Option<{path}>"), Reading::MissingSpread { path, presence })
    } else {
        (path.clone(), Reading::Spread { path, presence: spread.record.presence })
    };
    Ok(Field { name: field_name(header, alias)?, ty: RustType(ty), reading, column: span.start })
}

/// The field for the result column `column`, which comes from `item`.
fn column_field(
    header: &Header,
    core: &Core,
    nullable_from: &[bool],
    item: &Item,
    column: &sqlive_reads::ProbedColumn,
    position: usize,
    (sql, tokens): (&str, &[Token]),
) -> Result<Field, Cause> {
    let declared = header.declarations.iter().find(|declaration| declaration.column == column.name);
    // SQLite names the origin of a compound's first SELECT only.
    let origin = if core.compound { None } else { column.origin.as_ref() };
    let nullable = if item.kind == ItemKind::Subquery {
        // The later SELECTs of a compound give the column values too.
        core.compound || !never_null::expression(sql, &tokens[item.expression.clone()])
    } else {
        origin.is_some_and(|origin| !origin.never_null)
            || joins_with_nulls(
                core,
                nullable_from,
                item,
                origin,
                &column.name,
                header,
                declared.is_some(),
            )?
    };
    let (ty, reading) = match (declared.map(|declaration| &declaration.declared), origin) {
        (Some(Declared::Json(ty)), _) => {
            if matches!(item.kind, ItemKind::Expression | ItemKind::Subquery) {
                json::check(sql, tokens, item.tokens.clone(), &column.name, ty, &header.shapes)?;
            }
            (ty.clone(), Reading::Json)
        }
        (Some(Declared::Type(ty)), _) => {
            if nullable && !ty.is_option() {
                return Err(Cause::NullableColumn {
                    column: column.name.clone(),
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

/// For each FROM item, whether a LEFT, RIGHT or FULL join gives its columns
/// NULL for the rows it doesn't join.
fn nullable_items(core: &Core) -> Vec<bool> {
    (0..core.from.len())
        .map(|position| {
            let own = matches!(core.from[position].join, Join::Left | Join::Full);
            let later = core.from[position + 1..]
                .iter()
                .any(|item| matches!(item.join, Join::Right | Join::Full));
            own || later
        })
        .collect()
}

/// Whether a join gives the column NULL: the FROM item it comes from is
/// nullable.
fn joins_with_nulls(
    core: &Core,
    nullable_from: &[bool],
    item: &Item,
    origin: Option<&sqlive_reads::ColumnOrigin>,
    column: &str,
    header: &Header,
    declared: bool,
) -> Result<bool, Cause> {
    let qualifier = match &item.kind {
        ItemKind::Column { qualifier, .. } => qualifier.clone(),
        ItemKind::QualifiedStar(qualifier) => Some(qualifier.clone()),
        _ => None,
    };
    let Some(origin) = origin else { return Ok(false) };
    let reads = |from: &FromItem| match &qualifier {
        Some(qualifier) => from.qualifier.as_deref() == Some(qualifier.as_str()),
        None => from.table().is_some_and(|table| table.eq_ignore_ascii_case(&origin.table)),
    };
    let nullable: Vec<bool> = core
        .from
        .iter()
        .zip(nullable_from)
        .filter(|(from, _)| reads(from))
        .map(|(_, &nullable)| nullable)
        .collect();
    match nullable[..] {
        [] => Ok(false),
        [only] => Ok(only),
        [first, ..] if nullable.iter().all(|&flag| flag == first) => Ok(first),
        // The header's declaration says what the compiler can't tell.
        _ if declared => Ok(false),
        _ => {
            Err(Cause::UnplacedColumn { concerns: header.name.clone(), column: column.to_owned() })
        }
    }
}

#[cfg(test)]
#[path = "columns_tests.rs"]
mod tests;
