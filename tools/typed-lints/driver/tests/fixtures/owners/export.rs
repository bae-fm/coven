//! Stands in for an attribute macro that exports a function to an app, as
//! uniffi's `export` does: `#[export(Type)]` on `fn name() -> Type` writes
//! beside it `fn exported_name() -> Type`, which calls `name` and hands on
//! what it returns.

extern crate proc_macro;

use proc_macro::{TokenStream, TokenTree};

#[proc_macro_attribute]
pub fn export(returned: TokenStream, item: TokenStream) -> TokenStream {
    let mut tokens = item.clone().into_iter();
    let name = loop {
        match tokens.next() {
            Some(TokenTree::Ident(word)) if word.to_string() == "fn" => match tokens.next() {
                Some(TokenTree::Ident(name)) => break name.to_string(),
                _ => panic!("export: `fn` names no function"),
            },
            Some(_) => {}
            None => panic!("export: the item is no function"),
        }
    };
    let exported: TokenStream = format!("pub fn exported_{name}() -> {returned} {{ {name}() }}")
        .parse()
        .expect("export: the written function parses");
    let mut output = item;
    output.extend(exported);
    output
}
