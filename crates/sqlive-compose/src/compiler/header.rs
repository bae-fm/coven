//! The header of a `fragment!` or `read!` call (`SPEC.md`, section 11): its
//! name, parameters, parent row, relations and declarations, and its SQL.

use proc_macro2::TokenStream;
use syn::parse::{ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::{braced, parenthesized, token, Ident, LitStr, Token, Type};

mod keyword {
    syn::custom_keyword!(on);
    syn::custom_keyword!(with);
    syn::custom_keyword!(json);
}

/// Which macro a call is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CallKind {
    /// `fragment!`.
    Fragment,
    /// `read!`.
    Read,
}

/// A Rust type, as the call wrote it with its spacing normalized.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RustType(pub(crate) String);

impl RustType {
    fn of(ty: &Type) -> Self {
        let text = quote::ToTokens::to_token_stream(ty).to_string();
        let mut normal = text;
        for (from, to) in [
            (" < ", "<"),
            (" <", "<"),
            ("< ", "<"),
            (" >", ">"),
            (" :: ", "::"),
            (" ,", ","),
            ("& ", "&"),
        ] {
            normal = normal.replace(from, to);
        }
        Self(normal)
    }

    /// The type inside, when this is `Option<…>`.
    pub(crate) fn option_inner(&self) -> Option<RustType> {
        let Ok(Type::Path(path)) = syn::parse_str::<Type>(&self.0) else { return None };
        let segment = path.path.segments.last()?;
        if segment.ident != "Option" {
            return None;
        }
        let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else { return None };
        match arguments.args.first()? {
            syn::GenericArgument::Type(inner) => Some(Self::of(inner)),
            _ => None,
        }
    }

    /// Whether the type can hold NULL: it is `Option<…>`.
    pub(crate) fn is_option(&self) -> bool {
        self.option_inner().is_some()
    }
}

/// A parameter the request or a spread binds.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Parameter {
    pub(crate) name: String,
    pub(crate) ty: RustType,
}

/// The rows a fragment's caller gives it, as a table of its own.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Relation {
    pub(crate) name: String,
    /// Each column's name and STRICT type name.
    pub(crate) columns: Vec<(String, String)>,
}

/// What a declaration says of a column's value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Declared {
    /// A Rust type that reads the column.
    Type(RustType),
    /// A JSON document of the type.
    Json(RustType),
}

/// A column's declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Declaration {
    pub(crate) column: String,
    pub(crate) declared: Declared,
}

/// The type of a field of a JSON document's shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FieldType {
    /// A Rust type.
    Type(RustType),
    /// Bytes that the document holds as hexadecimal text.
    Hex,
}

/// The shape of a JSON document, which becomes a struct.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Shape {
    pub(crate) name: String,
    pub(crate) fields: Vec<(String, FieldType)>,
}

/// A parsed call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) kind: CallKind,
    pub(crate) name: String,
    pub(crate) parameters: Vec<Parameter>,
    pub(crate) parent: Option<String>,
    pub(crate) relations: Vec<Relation>,
    pub(crate) declarations: Vec<Declaration>,
    pub(crate) shapes: Vec<Shape>,
    pub(crate) sql: String,
}

/// Why a call's tokens aren't a header.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct HeaderError(String);

/// The call of kind `kind` that `tokens` hold.
pub(crate) fn parse(kind: CallKind, tokens: TokenStream) -> Result<Header, HeaderError> {
    let parser = |input: ParseStream<'_>| header(kind, input);
    parser.parse2(tokens).map_err(|error| HeaderError(error.to_string()))
}

fn header(kind: CallKind, input: ParseStream<'_>) -> syn::Result<Header> {
    let name: Ident = input.parse()?;
    let mut parameters = Vec::new();
    if input.peek(token::Paren) {
        let content;
        parenthesized!(content in input);
        let fields: Punctuated<(Ident, Type), Token![,]> = content.parse_terminated(
            |field| {
                Ok((
                    field.parse::<Ident>().and_then(|name| {
                        field.parse::<Token![:]>()?;
                        Ok(name)
                    })?,
                    field.parse::<Type>()?,
                ))
            },
            Token![,],
        )?;
        parameters = fields
            .into_iter()
            .map(|(name, ty)| Parameter { name: name.to_string(), ty: RustType::of(&ty) })
            .collect();
    }
    let mut parent = None;
    if input.peek(keyword::on) {
        let on: keyword::on = input.parse()?;
        if kind == CallKind::Read {
            return Err(syn::Error::new(on.span, "a read has no parent row"));
        }
        parent = Some(input.parse::<Ident>()?.to_string());
    }
    let mut relations = Vec::new();
    if input.peek(keyword::with) {
        let with: keyword::with = input.parse()?;
        if kind == CallKind::Read {
            return Err(syn::Error::new(with.span, "a read has no relations"));
        }
        loop {
            relations.push(relation(input)?);
            if input.peek(Token![,]) && !input.peek2(LitStr) {
                input.parse::<Token![,]>()?;
            } else {
                break;
            }
        }
    }
    let mut declarations = Vec::new();
    let mut shapes = Vec::new();
    if input.peek(token::Brace) {
        let content;
        braced!(content in input);
        while !content.is_empty() {
            if content.peek(Token![struct]) {
                shapes.push(shape(&content)?);
            } else {
                declarations.push(declaration(&content)?);
            }
            if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
            }
        }
    }
    input.parse::<Token![,]>()?;
    let sql: LitStr = input.parse()?;
    if input.peek(Token![,]) {
        input.parse::<Token![,]>()?;
    }
    if !input.is_empty() {
        return Err(input.error("nothing follows the SQL"));
    }
    Ok(Header {
        kind,
        name: name.to_string(),
        parameters,
        parent,
        relations,
        declarations,
        shapes,
        sql: sql.value(),
    })
}

fn relation(input: ParseStream<'_>) -> syn::Result<Relation> {
    let name: Ident = input.parse()?;
    let content;
    parenthesized!(content in input);
    let columns: Punctuated<(Ident, Ident), Token![,]> =
        content.parse_terminated(|column| Ok((column.parse()?, column.parse()?)), Token![,])?;
    Ok(Relation {
        name: name.to_string(),
        columns: columns.into_iter().map(|(name, ty)| (name.to_string(), ty.to_string())).collect(),
    })
}

fn declaration(input: ParseStream<'_>) -> syn::Result<Declaration> {
    let column: Ident = input.parse()?;
    input.parse::<Token![:]>()?;
    let declared = if input.peek(keyword::json) && input.peek2(Token![<]) {
        input.parse::<keyword::json>()?;
        input.parse::<Token![<]>()?;
        let ty: Type = input.parse()?;
        input.parse::<Token![>]>()?;
        Declared::Json(RustType::of(&ty))
    } else {
        Declared::Type(RustType::of(&input.parse::<Type>()?))
    };
    Ok(Declaration { column: column.to_string(), declared })
}

fn shape(input: ParseStream<'_>) -> syn::Result<Shape> {
    input.parse::<Token![struct]>()?;
    let name: Ident = input.parse()?;
    let content;
    braced!(content in input);
    let fields: Punctuated<(Ident, Type), Token![,]> = content.parse_terminated(
        |field| {
            let name: Ident = field.parse()?;
            field.parse::<Token![:]>()?;
            Ok((name, field.parse::<Type>()?))
        },
        Token![,],
    )?;
    let fields = fields
        .into_iter()
        .map(|(field, ty)| {
            let ty = RustType::of(&ty);
            let ty = if ty.0 == "hex" { FieldType::Hex } else { FieldType::Type(ty) };
            (field.to_string(), ty)
        })
        .collect();
    Ok(Shape { name: name.to_string(), fields })
}

#[cfg(test)]
#[path = "header_tests.rs"]
mod tests;
