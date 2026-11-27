//! The Rust that the compiler writes for a call: a fragment's type, or a
//! read's statement, parameters and row (`SPEC.md`, section 9).

use proc_macro2::{Literal, TokenStream};
use quote::{format_ident, quote};
use syn::Ident;

use super::header::{FieldType, Handle, Header, Parameter, RustType, Shape};
use super::model::{Field, FieldName, MacrosCrate, Reading};

/// The identifier of a name the header's parser read as one.
fn ident(name: &str) -> Ident {
    FieldName::new(name).expect("the header's parser read an identifier").ident()
}

fn rust_type(ty: &RustType) -> TokenStream {
    ty.0.parse().expect("a type the header's parser read, or one the compiler wrote")
}

fn path(macros: &MacrosCrate, tail: &str) -> TokenStream {
    macros.generated(tail).parse().expect("a path of a crate's name and constants")
}

/// The index of the column `column` places after the row's offset: `offset`
/// itself for the first column, which `offset + 0usize` would write as an
/// operation that clippy refuses (`clippy::identity_op`).
fn column_at(column: usize) -> TokenStream {
    if column == 0 {
        quote!(offset)
    } else {
        quote!(offset + #column)
    }
}

/// The expression that reads `field` from `row`, the column at `offset` plus
/// the field's own position.
fn read_field(field: &Field, macros: &MacrosCrate) -> TokenStream {
    let column = column_at(field.column);
    match &field.reading {
        Reading::Value => {
            let ty = rust_type(&field.ty);
            quote!(row.get::<_, #ty>(#column)?)
        }
        Reading::Json => {
            // SQL's NULL holds no JSON text, so an `Option` reads it apart.
            let reader = if field.ty.is_option() { "optional_json_column" } else { "json_column" };
            let json = path(macros, reader);
            quote!(#json(row, #column)?)
        }
        Reading::Spread { path: fragment, .. } => {
            let fragment = fragment_path(fragment);
            quote!(#fragment::read(row, #column)?)
        }
        Reading::MissingSpread { path: fragment, presence } => {
            let fragment = fragment_path(fragment);
            let at = column_at(field.column + presence);
            let is_null = path(macros, "is_null");
            quote!(if #is_null(row, #at)? { None } else { Some(#fragment::read(row, #column)?) })
        }
    }
}

/// The path of a fragment's type, which the columns' compile checked is one.
fn fragment_path(path: &str) -> syn::Path {
    syn::parse_str(path).expect("a fragment path that the compile of the columns checked")
}

fn row_type(macros: &MacrosCrate) -> TokenStream {
    let row = path(macros, "sqlive_reads::Row");
    quote!(&#row<'_>)
}

/// The structs of the JSON documents the header declares the shapes of, as
/// visible as `visibility` makes them.
fn shapes(shapes: &[Shape], visibility: &TokenStream, macros: &MacrosCrate) -> TokenStream {
    let serde = path(macros, "serde");
    let serde_text = macros.generated("serde");
    let hex = macros.generated("hex_bytes");
    let structs = shapes.iter().map(|shape| {
        let name = ident(&shape.name);
        let doc = format!("A JSON document of the shape `{}`.", shape.name);
        let fields = shape.fields.iter().map(|(field, ty)| {
            let field = ident(field);
            match ty {
                FieldType::Type(ty) => {
                    let ty = rust_type(ty);
                    quote!(pub(crate) #field: #ty,)
                }
                FieldType::Hex => quote!(
                    #[serde(deserialize_with = #hex)]
                    pub(crate) #field: Vec<u8>,
                ),
            }
        });
        quote! {
            #[doc = #doc]
            #[derive(Clone, Debug, PartialEq, #serde::Deserialize)]
            #[serde(crate = #serde_text)]
            #visibility struct #name {
                #(#fields)*
            }
        }
    });
    quote!(#(#structs)*)
}

/// `pub` when `exported`, which says that every module around a call is `pub`,
/// and `pub(crate)` otherwise.
fn visibility(exported: bool) -> TokenStream {
    if exported {
        quote!(pub)
    } else {
        quote!(pub(crate))
    }
}

/// A fragment's type, which reads its columns. Its items are `pub` when
/// `exported` and `pub(crate)` otherwise, as a read's are (`read`). The fields
/// are `pub(crate)` either way.
pub(crate) fn fragment(
    header: &Header,
    fields: &[Field],
    exported: bool,
    macros: &MacrosCrate,
) -> String {
    let visibility = visibility(exported);
    let name = ident(&header.name);
    let doc = format!("The columns of `{}`.", header.name);
    let names: Vec<Ident> = fields.iter().map(|field| field.name.ident()).collect();
    let types: Vec<TokenStream> = fields.iter().map(|field| rust_type(&field.ty)).collect();
    let reads: Vec<TokenStream> = fields.iter().map(|field| read_field(field, macros)).collect();
    let row = row_type(macros);
    let result = path(macros, "sqlive_reads::SqliteResult");
    let shapes = shapes(&header.shapes, &visibility, macros);
    quote! {
        #shapes

        #[doc = #doc]
        #[derive(Clone, Debug, PartialEq)]
        #visibility struct #name {
            #(pub(crate) #names: #types,)*
        }

        impl #name {
            /// Reads the fragment's columns from `row`, the first at `offset`.
            #visibility fn read(row: #row, offset: usize) -> #result<Self> {
                Ok(Self { #(#names: #reads,)* })
            }
        }
    }
    .to_string()
}

/// A read's statement, request and row, and `plan`, the statement's scope
/// plan as sqlive encodes it. Its items are `pub` when `exported`, which says
/// that every module around the call is `pub`, and `pub(crate)` otherwise, so
/// that a read in a private module has no `pub` item that nothing can reach
/// (`unreachable_pub`). The row derives what an app needs to keep and compare
/// it.
pub(crate) fn read(
    header: &Header,
    sql: &str,
    handle: Handle,
    plan: &str,
    fields: &[Field],
    exported: bool,
    macros: &MacrosCrate,
) -> String {
    statement(header, sql, fields, Form::Read { exported, handle, plan }, macros)
}

/// A write's statement and parameters, and its row when it returns columns.
/// Its items are `pub(crate)` whatever the modules around the call: only the
/// crate that writes a table's SQL changes it, so no other crate reaches the
/// write's items.
pub(crate) fn write(header: &Header, sql: &str, fields: &[Field], macros: &MacrosCrate) -> String {
    statement(header, sql, fields, Form::Write, macros)
}

/// What a generated statement is.
#[derive(Clone, Copy)]
enum Form<'a> {
    /// A read, whose items are `pub` when `exported`, with the handle that
    /// runs it and its scope plan.
    Read { exported: bool, handle: Handle, plan: &'a str },
    /// A write.
    Write,
}

/// The unit struct with the statement, the parameters struct, the row struct
/// when the statement returns columns, and the trait impls of the call.
fn statement(
    header: &Header,
    sql: &str,
    fields: &[Field],
    form: Form<'_>,
    macros: &MacrosCrate,
) -> String {
    let (visibility, noun) = match form {
        Form::Read { exported, .. } => (visibility(exported), "read"),
        Form::Write => (visibility(false), "write"),
    };
    let name = ident(&header.name);
    let params_name = format_ident!("{}Params", header.name);
    let row_name = format_ident!("{}Row", header.name);
    let doc = format!("The `{}` {noun}.", header.name);
    let sql = Literal::string(sql);
    let parameter_names: Vec<Ident> =
        header.parameters.iter().map(|parameter| ident(&parameter.name)).collect();
    let parameter_types: Vec<TokenStream> =
        header.parameters.iter().map(|Parameter { ty, .. }| rust_type(ty)).collect();
    let bound: Vec<String> =
        header.parameters.iter().map(|parameter| format!(":{}", parameter.name)).collect();
    let lifetime = if header.borrows() { quote!(<'p>) } else { quote!() };
    let to_sql = path(macros, "sqlive_reads::ToSql");
    let statement = path(macros, "Statement");
    let params_doc = match form {
        Form::Read { .. } => format!("What the `{}` read's request binds.", header.name),
        Form::Write => format!("What the `{}` write binds.", header.name),
    };
    let shapes = shapes(&header.shapes, &visibility, macros);
    let rows = if fields.is_empty() {
        let write_trait = path(macros, "Write");
        quote!(impl #lifetime #write_trait for #params_name #lifetime {})
    } else {
        let names: Vec<Ident> = fields.iter().map(|field| field.name.ident()).collect();
        let types: Vec<TokenStream> = fields.iter().map(|field| rust_type(&field.ty)).collect();
        let reads: Vec<TokenStream> =
            fields.iter().map(|field| read_field(field, macros)).collect();
        let row = row_type(macros);
        let result = path(macros, "sqlive_reads::SqliteResult");
        let row_doc = format!("A row of the `{}` {noun}.", header.name);
        let returning = path(macros, "Returning");
        let marker = match form {
            Form::Read { handle, plan, .. } => {
                let handle = match handle {
                    Handle::Writer => format_ident!("Writer"),
                    Handle::ReadOnly => format_ident!("ReadOnly"),
                };
                let handles = path(macros, "sqlive_reads");
                let plan = Literal::string(plan);
                let read_trait = path(macros, "Read");
                quote! {
                    impl #lifetime #read_trait for #params_name #lifetime {
                        type Handle = #handles::#handle;

                        const PLAN: &'static str = #plan;
                    }
                }
            }
            Form::Write => {
                let write_trait = path(macros, "WriteReturning");
                quote!(impl #lifetime #write_trait for #params_name #lifetime {})
            }
        };
        quote! {
            #[doc = #row_doc]
            #[derive(Clone, Debug, PartialEq)]
            #visibility struct #row_name {
                #(
                    #[allow(missing_docs)]
                    #visibility #names: #types,
                )*
            }

            impl #lifetime #returning for #params_name #lifetime {
                type Row = #row_name;

                fn read(row: #row) -> #result<#row_name> {
                    let offset = 0;
                    Ok(#row_name { #(#names: #reads,)* })
                }
            }

            #marker
        }
    };
    quote! {
        #shapes

        #[doc = #doc]
        #visibility struct #name;

        impl #name {
            /// The composed statement.
            #visibility const SQL: &'static str = #sql;
        }

        #[doc = #params_doc]
        #visibility struct #params_name #lifetime {
            #(
                #[allow(missing_docs)]
                #visibility #parameter_names: #parameter_types,
            )*
        }

        impl #lifetime #statement for #params_name #lifetime {
            const SQL: &'static str = #name::SQL;

            fn params(&self) -> Vec<(&'static str, &dyn #to_sql)> {
                vec![#((#bound, &self.#parameter_names as &dyn #to_sql),)*]
            }
        }

        #rows
    }
    .to_string()
}

#[cfg(test)]
#[path = "generate_tests.rs"]
mod tests;
