//! The typed columns of a call's statement: where each comes from in a
//! table, whether it can be NULL, and the Rust type that reads it.

use super::core::{Core, Item, ItemKind, Join};
use super::error::Cause;
use super::header::{Declared, Header, RustType};
use super::model::{strict_type, Field, Reading};

/// Each result column of the statement, as `prepared` describes it, as a
/// field of the call's type, in order.
pub(crate) fn fields(
    header: &Header,
    core: &Core,
    prepared: &sqlive::ProbedStatement,
) -> Result<Vec<Field>, Cause> {
    let items = align(header, core, prepared.columns.len())?;
    let nullable_from = nullable_items(core);
    let mut fields = Vec::new();
    for (position, (column, &item)) in prepared.columns.iter().zip(&items).enumerate() {
        if fields.iter().any(|field: &Field| field.name == column.name) {
            return Err(Cause::DuplicateColumn {
                concerns: header.name.clone(),
                column: column.name.clone(),
            });
        }
        let declared =
            header.declarations.iter().find(|declaration| declaration.column == column.name);
        let nullable = column.origin.as_ref().is_some_and(|origin| !origin.not_null)
            || joins_with_nulls(core, &nullable_from, item, column, header)?
            || item.is_some_and(|item| item.kind == ItemKind::Subquery);
        let (ty, reading) =
            match (declared.map(|declaration| &declaration.declared), &column.origin) {
                (Some(Declared::Json(ty)), _) => (ty.clone(), Reading::Json),
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
        fields.push(Field { name: column.name.clone(), ty, reading, column: position });
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

/// The result item each column comes from: a column an item names gives that
/// item, and a `*` gives the columns it expands to, whose number is what the
/// other items leave. Nothing aligns when the core is a compound.
fn align<'a>(
    header: &Header,
    core: &'a Core,
    columns: usize,
) -> Result<Vec<Option<&'a Item>>, Cause> {
    if core.compound {
        return Ok(vec![None; columns]);
    }
    let stars = core
        .items
        .iter()
        .filter(|item| matches!(item.kind, ItemKind::Star | ItemKind::QualifiedStar(_)))
        .count();
    if stars > 1 {
        return Err(Cause::Stars { concerns: header.name.clone() });
    }
    let single = core.items.len() - stars;
    let star_width = columns.saturating_sub(single);
    let mut aligned = Vec::new();
    for item in &core.items {
        let width = if matches!(item.kind, ItemKind::Star | ItemKind::QualifiedStar(_)) {
            star_width
        } else {
            1
        };
        aligned.extend(std::iter::repeat_n(Some(item), width));
    }
    aligned.resize(columns, None);
    Ok(aligned)
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
    item: Option<&Item>,
    column: &sqlive::ProbedColumn,
    header: &Header,
) -> Result<bool, Cause> {
    let qualifier = match item.map(|item| &item.kind) {
        Some(ItemKind::Column { qualifier, .. }) => qualifier.clone(),
        Some(ItemKind::QualifiedStar(qualifier)) => Some(qualifier.clone()),
        _ => None,
    };
    let Some(origin) = &column.origin else { return Ok(false) };
    let candidates: Vec<usize> = core
        .from
        .iter()
        .enumerate()
        .filter(|(_, from)| match &qualifier {
            Some(qualifier) => from.qualifier.as_deref() == Some(qualifier.as_str()),
            None => {
                from.table.as_deref().is_some_and(|table| table.eq_ignore_ascii_case(&origin.table))
            }
        })
        .map(|(position, _)| position)
        .collect();
    let nullable: Vec<bool> = candidates.iter().map(|&position| nullable_from[position]).collect();
    match nullable[..] {
        [] => Ok(false),
        [only] => Ok(only),
        [first, ..] if nullable.iter().all(|&flag| flag == first) => Ok(first),
        _ => Err(Cause::UnplacedColumn {
            concerns: header.name.clone(),
            column: column.name.clone(),
        }),
    }
}

#[cfg(test)]
#[path = "columns_tests.rs"]
mod tests;
