//! The `fragment!`, `read!` and `write!` macros of sqlive-compose. Each expands to one
//! line that includes the Rust the compiler wrote for the call while the app's
//! build script ran (`sqlive-compose`, section 10 of its spec). The macros
//! read nothing of the call but its name, which is the generated file's name.

use proc_macro::TokenStream;
use proc_macro2::TokenTree;
use quote::quote;

/// Offers a SQL `SELECT` to the reads that spread it: `fragment!(Name …,
/// r#"SQL"#)`.
#[proc_macro]
pub fn fragment(input: TokenStream) -> TokenStream {
    include_generated(input.into(), "fragment!").into()
}

/// Declares a read that an app runs as a live query: `read!(Name …,
/// r#"SQL"#)`.
#[proc_macro]
pub fn read(input: TokenStream) -> TokenStream {
    include_generated(input.into(), "read!").into()
}

/// Declares a write that an app runs on its store: `write!(Name …,
/// r#"SQL"#)`.
#[proc_macro]
pub fn write(input: TokenStream) -> TokenStream {
    include_generated(input.into(), "write!").into()
}

/// The line that includes the file for the call whose tokens are `input`.
fn include_generated(
    input: proc_macro2::TokenStream,
    macro_name: &str,
) -> proc_macro2::TokenStream {
    let Some(TokenTree::Ident(name)) = input.into_iter().next() else {
        let message = format!("{macro_name} starts with the name of its call");
        return syn::Error::new(proc_macro2::Span::call_site(), message).to_compile_error();
    };
    let file = format!("/sqlive_compose/{name}.rs");
    quote!(include!(concat!(env!("OUT_DIR"), #file));)
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
