//! Substitute fixed, comma-separated macro arguments when an item macro
//! declares names. Unsupported matchers remain unexpanded; references to names
//! absent from the syntax index then produce ordinary resolution errors.

use proc_macro2::{Group, TokenStream, TokenTree};
use std::collections::BTreeMap;
use syn::parse::Parser;

pub(crate) fn expand(definition: &syn::Macro, invocation: &syn::Macro) -> Option<syn::File> {
    let rules = definition.tokens.clone().into_iter().collect::<Vec<_>>();
    let mut offset = 0;
    while offset < rules.len() {
        let (
            Some(TokenTree::Group(pattern)),
            Some(TokenTree::Punct(equals)),
            Some(TokenTree::Punct(arrow)),
            Some(TokenTree::Group(body)),
        ) = (
            rules.get(offset),
            rules.get(offset + 1),
            rules.get(offset + 2),
            rules.get(offset + 3),
        )
        else {
            return None;
        };
        if equals.as_char() != '=' || arrow.as_char() != '>' {
            return None;
        }
        match match_arguments(pattern.stream(), invocation.tokens.clone()) {
            Arguments::Matched(arguments) => {
                let tokens = substitute(
                    body.stream(),
                    &arguments,
                    invocation.path.segments.last()?.ident.span(),
                )?;
                return syn::parse2(tokens).ok();
            }
            Arguments::Unsupported => return None,
            Arguments::Different => {}
        }
        offset += 4;
        if matches!(rules.get(offset), Some(TokenTree::Punct(punct)) if matches!(punct.as_char(), ';' | ','))
        {
            offset += 1;
        }
    }
    None
}

enum Arguments {
    Matched(BTreeMap<String, TokenStream>),
    Different,
    Unsupported,
}

fn match_arguments(pattern: TokenStream, tokens: TokenStream) -> Arguments {
    let trailing_comma = matches!(pattern.clone().into_iter().last(), Some(TokenTree::Punct(punct)) if punct.as_char() == ',');
    let mut fragments = Vec::new();
    for pattern in comma_parts(pattern) {
        let tokens = pattern.into_iter().collect::<Vec<_>>();
        let [TokenTree::Punct(dollar), TokenTree::Ident(name), TokenTree::Punct(colon), TokenTree::Ident(fragment)] =
            tokens.as_slice()
        else {
            return Arguments::Unsupported;
        };
        let fragment = fragment.to_string();
        if dollar.as_char() != '$'
            || colon.as_char() != ':'
            || !matches!(fragment.as_str(), "ident" | "ty" | "path" | "literal" | "expr")
        {
            return Arguments::Unsupported;
        }
        fragments.push((name.to_string(), fragment));
    }
    let parser = |input: syn::parse::ParseStream<'_>| {
        let mut arguments = BTreeMap::new();
        for (index, (name, fragment)) in fragments.iter().enumerate() {
            if index > 0 {
                input.parse::<syn::Token![,]>()?;
            }
            let mut cursor = input.cursor();
            match fragment.as_str() {
                "ident" => {
                    input.parse::<syn::Ident>()?;
                }
                "ty" => {
                    input.parse::<syn::Type>()?;
                }
                "path" => {
                    input.parse::<syn::Path>()?;
                }
                "literal" => {
                    input.parse::<syn::Lit>()?;
                }
                "expr" => {
                    input.parse::<syn::Expr>()?;
                }
                _ => unreachable!("validated macro fragment"),
            }
            let mut captured = TokenStream::new();
            while cursor != input.cursor() {
                let (token, next) = cursor.token_tree().expect("parsed fragment tokens");
                captured.extend([token]);
                cursor = next;
            }
            arguments.insert(name.clone(), captured);
        }
        if trailing_comma {
            input.parse::<syn::Token![,]>()?;
        }
        if input.is_empty() {
            Ok(arguments)
        } else {
            Err(input.error("unexpected macro argument"))
        }
    };
    match parser.parse2(tokens) {
        Ok(arguments) => Arguments::Matched(arguments),
        Err(_) => Arguments::Different,
    }
}

fn comma_parts(tokens: TokenStream) -> Vec<TokenStream> {
    let mut parts = Vec::new();
    let mut part = TokenStream::new();
    for token in tokens {
        if matches!(&token, TokenTree::Punct(punct) if punct.as_char() == ',') {
            parts.push(part);
            part = TokenStream::new();
        } else {
            part.extend([token]);
        }
    }
    if !part.is_empty() {
        parts.push(part);
    }
    parts
}

fn substitute(
    tokens: TokenStream,
    arguments: &BTreeMap<String, TokenStream>,
    span: proc_macro2::Span,
) -> Option<TokenStream> {
    let mut output = TokenStream::new();
    let mut tokens = tokens.into_iter();
    while let Some(mut token) = tokens.next() {
        match token {
            TokenTree::Punct(ref punct) if punct.as_char() == '$' => {
                let TokenTree::Ident(name) = tokens.next()? else {
                    return None;
                };
                output.extend(arguments.get(&name.to_string())?.clone());
            }
            TokenTree::Group(group) => {
                let mut expanded =
                    Group::new(group.delimiter(), substitute(group.stream(), arguments, span)?);
                expanded.set_span(span);
                output.extend([TokenTree::Group(expanded)]);
            }
            _ => {
                token.set_span(span);
                output.extend([token]);
            }
        }
    }
    Some(output)
}

#[cfg(test)]
#[path = "macro_expansion_tests.rs"]
mod tests;
