//! The Rust that the compiler writes for a call: a fragment's type, or a
//! read's statement, parameters and row (`SPEC.md`, section 9).

use proc_macro2::{Literal, TokenStream};
use quote::{format_ident, quote};
use syn::Ident;

use super::header::{Header, Parameter, RustType};
use super::model::{Field, Reading};

const GENERATED: &str = "::sqlive_compose::generated";

/// An identifier for `name`, raw when `name` is a Rust keyword.
fn ident(name: &str) -> Ident {
    syn::parse_str::<Ident>(name)
        .unwrap_or_else(|_| Ident::new_raw(name, proc_macro2::Span::call_site()))
}

fn rust_type(ty: &RustType) -> TokenStream {
    ty.0.parse().unwrap_or_else(|_| quote!(compile_error!("the type is not Rust")))
}

fn path(tail: &str) -> TokenStream {
    format!("{GENERATED}::{tail}").parse().unwrap_or_default()
}

/// The expression that reads `field` from `row`, the column at `offset` plus
/// the field's own position.
fn read_field(field: &Field) -> TokenStream {
    let column = field.column;
    match &field.reading {
        Reading::Value => {
            let ty = rust_type(&field.ty);
            quote!(row.get::<_, #ty>(offset + #column)?)
        }
        Reading::Json => {
            let json = path("json_column");
            quote!(#json(row, offset + #column)?)
        }
        Reading::Spread { path: fragment, presence } => {
            let fragment: TokenStream = fragment.parse().unwrap_or_default();
            if field.ty.is_option() {
                let at = column + presence.unwrap_or_default();
                let is_null = path("is_null");
                quote!(if #is_null(row, offset + #at)? { None } else { Some(#fragment::read(row, offset + #column)?) })
            } else {
                quote!(#fragment::read(row, offset + #column)?)
            }
        }
    }
}

fn row_type() -> TokenStream {
    let row = path("sqlive::Row");
    quote!(&#row<'_>)
}

/// A fragment's type, which reads its columns.
pub(crate) fn fragment(header: &Header, fields: &[Field]) -> String {
    let name = ident(&header.name);
    let doc = format!("The columns of `{}`.", header.name);
    let names: Vec<Ident> = fields.iter().map(|field| ident(&field.name)).collect();
    let types: Vec<TokenStream> = fields.iter().map(|field| rust_type(&field.ty)).collect();
    let reads: Vec<TokenStream> = fields.iter().map(read_field).collect();
    let row = row_type();
    let result = path("sqlive::SqliteResult");
    quote! {
        #[doc = #doc]
        pub struct #name {
            #(pub(crate) #names: #types,)*
        }

        impl #name {
            /// Reads the fragment's columns from `row`, the first at `offset`.
            pub fn read(row: #row, offset: usize) -> #result<Self> {
                Ok(Self { #(#names: #reads,)* })
            }
        }
    }
    .to_string()
}

/// A read's statement, request and row.
pub(crate) fn read(header: &Header, sql: &str, fields: &[Field]) -> String {
    let name = ident(&header.name);
    let params_name = format_ident!("{}Params", header.name);
    let row_name = format_ident!("{}Row", header.name);
    let doc = format!("The `{}` read.", header.name);
    let sql = Literal::string(sql);
    let parameter_names: Vec<Ident> =
        header.parameters.iter().map(|parameter| ident(&parameter.name)).collect();
    let parameter_types: Vec<TokenStream> =
        header.parameters.iter().map(|Parameter { ty, .. }| rust_type(ty)).collect();
    let bound: Vec<String> =
        header.parameters.iter().map(|parameter| format!(":{}", parameter.name)).collect();
    let names: Vec<Ident> = fields.iter().map(|field| ident(&field.name)).collect();
    let types: Vec<TokenStream> = fields.iter().map(|field| rust_type(&field.ty)).collect();
    let reads: Vec<TokenStream> = fields.iter().map(read_field).collect();
    let row = row_type();
    let result = path("sqlive::SqliteResult");
    let to_sql = path("sqlive::ToSql");
    let params_doc = format!("What the `{}` read's request binds.", header.name);
    let row_doc = format!("A row of the `{}` read.", header.name);
    quote! {
        #[doc = #doc]
        pub struct #name;

        impl #name {
            /// The composed statement.
            pub const SQL: &'static str = #sql;
        }

        #[doc = #params_doc]
        pub struct #params_name {
            #(
                #[allow(missing_docs)]
                pub #parameter_names: #parameter_types,
            )*
        }

        impl #params_name {
            /// The parameters by their names in the statement.
            pub fn params(&self) -> Vec<(&'static str, &dyn #to_sql)> {
                vec![#((#bound, &self.#parameter_names as &dyn #to_sql),)*]
            }
        }

        #[doc = #row_doc]
        pub struct #row_name {
            #(
                #[allow(missing_docs)]
                pub #names: #types,
            )*
        }

        impl #row_name {
            /// Reads a row of the statement.
            pub fn read(row: #row) -> #result<Self> {
                let offset = 0;
                Ok(Self { #(#names: #reads,)* })
            }
        }
    }
    .to_string()
}

#[cfg(test)]
#[path = "generate_tests.rs"]
mod tests;
