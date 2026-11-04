//! The string literals of a Rust source, for the plan audit to explain as SQL.

use proc_macro2::{TokenStream, TokenTree};

pub(super) fn literals(tokens: TokenStream, strings: &mut Vec<(usize, String)>) {
    // A doc comment reaches the tokens as `#[doc = "…"]` (`#![doc …]` for an
    // inner one); it is prose, so its string is not read.
    let mut hash = false;
    for token in tokens {
        let after_hash = std::mem::replace(&mut hash, false);
        match token {
            TokenTree::Punct(punct)
                if punct.as_char() == '#' || (after_hash && punct.as_char() == '!') =>
            {
                hash = true;
            }
            TokenTree::Group(group)
                if after_hash
                    && group.delimiter() == proc_macro2::Delimiter::Bracket
                    && matches!(group.stream().into_iter().next(),
                        Some(TokenTree::Ident(name)) if name == "doc") => {}
            TokenTree::Group(group) => literals(group.stream(), strings),
            TokenTree::Literal(literal) => {
                if let Ok(syn::Lit::Str(string)) = syn::parse_str::<syn::Lit>(&literal.to_string())
                {
                    strings.push((literal.span().start().line, string.value()));
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::literals;

    /// The audit explains string literals, never the comments around them: a doc
    /// comment that opens with an SQL verb is prose, not a statement.
    #[test]
    fn doc_comments_are_not_sql_literals() {
        let text = r#"
            //! Select what the module keeps.
            /// Delete what hangs off a row.
            #[doc = "Update the row."]
            fn drop_row() {
                // Insert nothing.
                let _ = "DELETE FROM _coven_pins";
            }
        "#;
        let mut strings = Vec::new();
        literals(text.parse().unwrap(), &mut strings);
        let found: Vec<&str> = strings.iter().map(|(_, string)| string.as_str()).collect();
        assert_eq!(found, ["DELETE FROM _coven_pins"]);
    }
}
