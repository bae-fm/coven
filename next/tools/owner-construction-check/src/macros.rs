//! Macro calls (§21.4: the checker reads the syntax tree "including inside
//! macro calls"). A macro's arguments reach `syn` as unparsed tokens, so a
//! visitor never enters them on its own.
//!
//! When the arguments parse as comma-separated expressions or as statements —
//! the shape of nearly every call: `format!`, `vec!`, `assert!`, `write!` —
//! a rule visits them as code, with every rule it applies elsewhere. When they
//! don't, as in a `macro_rules!` body, the rule reads the tokens: the paths,
//! the calls and the string literals in them.

use proc_macro2::{Delimiter, Span, TokenStream, TokenTree};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::visit::Visit;

pub(crate) enum MacroBody {
    Expressions(Punctuated<syn::Expr, syn::Token![,]>),
    Statements(Vec<syn::Stmt>),
}

pub(crate) fn parse_macro_body(node: &syn::Macro) -> Option<MacroBody> {
    if let Ok(expressions) =
        Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated.parse2(node.tokens.clone())
    {
        return Some(MacroBody::Expressions(expressions));
    }
    syn::Block::parse_within
        .parse2(node.tokens.clone())
        .ok()
        .map(MacroBody::Statements)
}

impl MacroBody {
    pub(crate) fn visit<V>(&self, visitor: &mut V)
    where
        V: for<'ast> Visit<'ast>,
    {
        match self {
            MacroBody::Expressions(expressions) => {
                for expression in expressions {
                    visitor.visit_expr(expression);
                }
            }
            MacroBody::Statements(statements) => {
                for statement in statements {
                    visitor.visit_stmt(statement);
                }
            }
        }
    }
}

/// A path in a token stream: a run of identifiers joined by `::`, and what
/// directly follows it.
pub(crate) struct TokenPath {
    pub(crate) segments: Vec<String>,
    pub(crate) span: Span,
    /// The delimiter of the group right after the path: `(` for a call, `{`
    /// for a struct literal.
    pub(crate) followed_by: Option<Delimiter>,
    /// Whether the path is a method name after `.`.
    pub(crate) after_dot: bool,
    /// The identifier before the `.` when the path is a method name.
    pub(crate) receiver: Option<String>,
}

/// Every path in `tokens`, nested groups included. A leading `::` is dropped.
pub(crate) fn token_paths(tokens: TokenStream) -> Vec<TokenPath> {
    let mut paths = Vec::new();
    collect_token_paths(tokens, &mut paths);
    paths
}

fn collect_token_paths(tokens: TokenStream, paths: &mut Vec<TokenPath>) {
    let trees = tokens.into_iter().collect::<Vec<_>>();
    let mut index = 0;
    while index < trees.len() {
        match &trees[index] {
            TokenTree::Group(group) => {
                collect_token_paths(group.stream(), paths);
                index += 1;
            }
            TokenTree::Ident(ident) => {
                let mut segments = vec![ident.to_string()];
                let mut cursor = index + 1;
                while let Some(next) = separator_then_ident(&trees, cursor) {
                    segments.push(next.to_string());
                    cursor += 3;
                }
                let after_dot = index >= 1
                    && matches!(&trees[index - 1], TokenTree::Punct(punct) if punct.as_char() == '.');
                let receiver = match (after_dot, index.checked_sub(2).map(|before| &trees[before]))
                {
                    (true, Some(TokenTree::Ident(receiver))) => Some(receiver.to_string()),
                    _ => None,
                };
                let followed_by = match trees.get(cursor) {
                    Some(TokenTree::Group(group)) => Some(group.delimiter()),
                    _ => None,
                };
                paths.push(TokenPath {
                    segments,
                    span: ident.span(),
                    followed_by,
                    after_dot,
                    receiver,
                });
                index = cursor;
            }
            _ => index += 1,
        }
    }
}

/// The identifier at `index + 2` when the tokens at `index` are `:: ident`.
fn separator_then_ident(trees: &[TokenTree], index: usize) -> Option<&proc_macro2::Ident> {
    let (TokenTree::Punct(first), TokenTree::Punct(second)) =
        (trees.get(index)?, trees.get(index + 1)?)
    else {
        return None;
    };
    if first.as_char() != ':'
        || second.as_char() != ':'
        || first.spacing() != proc_macro2::Spacing::Joint
    {
        return None;
    }
    match trees.get(index + 2)? {
        TokenTree::Ident(ident) => Some(ident),
        _ => None,
    }
}

/// Every string literal in `tokens`, nested groups included.
pub(crate) fn token_strings(tokens: TokenStream) -> Vec<syn::LitStr> {
    let mut strings = Vec::new();
    for tree in tokens {
        match tree {
            TokenTree::Group(group) => strings.extend(token_strings(group.stream())),
            TokenTree::Literal(literal) => {
                if let Ok(string) = syn::parse2::<syn::LitStr>(TokenTree::Literal(literal).into()) {
                    strings.push(string);
                }
            }
            _ => {}
        }
    }
    strings
}

#[cfg(test)]
#[path = "macros_tests.rs"]
mod tests;
