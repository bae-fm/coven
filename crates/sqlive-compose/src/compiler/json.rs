//! The keys of a JSON document that a column builds, compared with the shape
//! that its header declares for it (`SPEC.md`, section 8).

use std::ops::Range;

use super::core::matching;
use super::error::Cause;
use super::header::{FieldType, RustType, Shape};
use super::lex::{Kind, Token};

/// What the header says a document holds.
enum Expected<'a> {
    /// An array of documents.
    Array(Box<Expected<'a>>),
    /// An object of a declared shape.
    Object(&'a Shape),
    /// A value the compiler doesn't look into.
    Opaque,
}

/// What the SQL builds.
enum Built {
    /// An array of documents.
    Array(Box<Built>),
    /// An object, by the key and the tokens of each value.
    Object(Vec<(String, Range<usize>)>),
    /// Something the compiler can't read keys from.
    Unknown,
}

/// Whether the document that `tokens[range]` builds, which the column
/// `column` holds, has the keys of the shape that `declared` names.
pub(crate) fn check(
    sql: &str,
    tokens: &[Token],
    range: Range<usize>,
    column: &str,
    declared: &RustType,
    shapes: &[Shape],
) -> Result<(), Cause> {
    let expected = declared_document(declared, shapes);
    compare(sql, tokens, shapes, &expected, &built_document(sql, tokens, range), column)
}

fn declared_document<'a>(ty: &RustType, shapes: &'a [Shape]) -> Expected<'a> {
    if let Some(inner) = ty.option_inner() {
        return declared_document(&inner, shapes);
    }
    if let Some(inner) = ty.vec_inner() {
        return Expected::Array(Box::new(declared_document(&inner, shapes)));
    }
    shapes.iter().find(|shape| shape.name == ty.0).map_or(Expected::Opaque, Expected::Object)
}

fn compare(
    sql: &str,
    tokens: &[Token],
    shapes: &[Shape],
    expected: &Expected<'_>,
    built: &Built,
    column: &str,
) -> Result<(), Cause> {
    match (expected, built) {
        (Expected::Opaque, _) | (_, Built::Unknown) => Ok(()),
        (Expected::Array(inner), Built::Array(element)) => {
            compare(sql, tokens, shapes, inner, element, column)
        }
        (Expected::Object(shape), Built::Object(pairs)) => {
            let extra: Vec<String> = pairs
                .iter()
                .filter(|(key, _)| !shape.fields.iter().any(|(field, _)| field == key))
                .map(|(key, _)| key.clone())
                .collect();
            let missing: Vec<String> = shape
                .fields
                .iter()
                .filter(|(field, _)| !pairs.iter().any(|(key, _)| key == field))
                .map(|(field, _)| field.clone())
                .collect();
            if !extra.is_empty() || !missing.is_empty() {
                return Err(Cause::JsonKeys {
                    column: column.to_owned(),
                    shape: shape.name.clone(),
                    extra,
                    missing,
                });
            }
            for (field, ty) in &shape.fields {
                let Some((_, value)) = pairs.iter().find(|(key, _)| key == field) else { continue };
                if let FieldType::Type(ty) = ty {
                    let inner = built_document(sql, tokens, value.clone());
                    compare(sql, tokens, shapes, &declared_document(ty, shapes), &inner, column)?;
                }
            }
            Ok(())
        }
        (Expected::Array(_), Built::Object(_)) => Err(Cause::JsonKind {
            column: column.to_owned(),
            declared: "an array".to_owned(),
            built: "an object".to_owned(),
        }),
        (Expected::Object(_), Built::Array(_)) => Err(Cause::JsonKind {
            column: column.to_owned(),
            declared: "an object".to_owned(),
            built: "an array".to_owned(),
        }),
    }
}

/// The document that the first JSON constructor in `tokens[range]` builds.
fn built_document(sql: &str, tokens: &[Token], range: Range<usize>) -> Built {
    for index in range.clone() {
        let token = &tokens[index];
        let calls = tokens.get(index + 1).is_some_and(|next| next.is_symbol(sql, '('));
        if token.kind != Kind::Word || !calls {
            continue;
        }
        let Some(close) = matching(sql, tokens, index + 1).filter(|close| *close < range.end)
        else {
            continue;
        };
        match token.text(sql).to_ascii_lowercase().as_str() {
            "json_group_array" | "jsonb_group_array" => {
                let element = element_range(sql, tokens, index + 2..close);
                return Built::Array(Box::new(built_document(sql, tokens, element)));
            }
            "json_object" | "jsonb_object" => return object(sql, tokens, index + 2..close),
            _ => {}
        }
    }
    Built::Unknown
}

/// The tokens of the first argument of an aggregate, which end at a comma
/// or an ORDER BY of its own.
fn element_range(sql: &str, tokens: &[Token], range: Range<usize>) -> Range<usize> {
    let mut depth = 0usize;
    for index in range.clone() {
        let token = &tokens[index];
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && (token.is_symbol(sql, ',') || token.is_word(sql, "order")) {
            return range.start..index;
        }
    }
    range
}

fn object(sql: &str, tokens: &[Token], range: Range<usize>) -> Built {
    let mut arguments = Vec::new();
    let mut depth = 0usize;
    let mut start = range.start;
    for index in range.clone() {
        let token = &tokens[index];
        if token.is_symbol(sql, '(') {
            depth += 1;
        } else if token.is_symbol(sql, ')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && token.is_symbol(sql, ',') {
            arguments.push(start..index);
            start = index + 1;
        }
    }
    if start < range.end {
        arguments.push(start..range.end);
    }
    let mut pairs = Vec::new();
    for pair in arguments.chunks(2) {
        let [key, value] = pair else { return Built::Unknown };
        let key_tokens = &tokens[key.clone()];
        let [literal] = key_tokens else { return Built::Unknown };
        if literal.kind != Kind::Text {
            return Built::Unknown;
        }
        let text = literal.text(sql);
        pairs.push((text[1..text.len() - 1].replace("''", "'"), value.clone()));
    }
    Built::Object(pairs)
}
