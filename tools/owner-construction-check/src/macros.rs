//! Macro calls (Appendix F.6: the checker reads the syntax tree "including inside
//! macro calls"). The workspace's own macros are expanded before the rules
//! run (`macro_expansion.rs`); what remains is other crates' macros, and any
//! invocation the checker could not expand. Their arguments reach `syn` as
//! unparsed tokens, so a visitor never enters them on its own.
//!
//! When the arguments parse as comma-separated expressions or as statements —
//! the shape of nearly every call: `format!`, `vec!`, `assert!`, `write!` —
//! a rule visits them as code, with every rule it applies elsewhere. A
//! `matches!` call is an expression and a pattern, visited as each. When they
//! don't, as in `tokio::select!`, the rule reads the tokens: the paths, the
//! calls and the string literals in them, each path marked as a type or a
//! value by the tokens around it, so the construction rule counts only values.
//! `cfg!` and `stringify!` take no code, and are read as neither.

use proc_macro2::{Delimiter, Span, TokenStream, TokenTree};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::visit::Visit;

pub(crate) enum MacroBody {
    Expressions(Punctuated<syn::Expr, syn::Token![,]>),
    Statements(Vec<syn::Stmt>),
    /// `matches!(value, pattern if guard)`: the pattern matches the value and
    /// constructs none.
    Match {
        value: syn::Expr,
        pattern: syn::Pat,
        guard: Option<syn::Expr>,
    },
}

/// Whether a macro's arguments are no code: `cfg!` reads a configuration
/// predicate, whose `any(...)` and `all(...)` are no function calls, and
/// `stringify!` turns its tokens into text.
fn reads_no_code(node: &syn::Macro) -> bool {
    node.path.is_ident("cfg") || node.path.is_ident("stringify")
}

/// The tokens a rule reads when a macro's arguments do not parse as code:
/// none for a macro whose arguments are no code.
pub(crate) fn unparsed_tokens(node: &syn::Macro) -> TokenStream {
    if reads_no_code(node) {
        TokenStream::new()
    } else {
        node.tokens.clone()
    }
}

pub(crate) fn parse_macro_body(node: &syn::Macro) -> Option<MacroBody> {
    if reads_no_code(node) {
        return None;
    }
    if node.path.segments.last().is_some_and(|segment| segment.ident == "matches") {
        if let Ok(body) = parse_match.parse2(node.tokens.clone()) {
            return Some(body);
        }
    }
    if let Ok(expressions) =
        Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated.parse2(node.tokens.clone())
    {
        return Some(MacroBody::Expressions(expressions));
    }
    syn::Block::parse_within.parse2(node.tokens.clone()).ok().map(MacroBody::Statements)
}

/// `value, pattern`, with an optional `if guard` and trailing comma.
fn parse_match(input: syn::parse::ParseStream<'_>) -> syn::Result<MacroBody> {
    let value = input.parse()?;
    input.parse::<syn::Token![,]>()?;
    let pattern = syn::Pat::parse_multi_with_leading_vert(input)?;
    let guard = if input.parse::<Option<syn::Token![if]>>()?.is_some() {
        Some(input.parse()?)
    } else {
        None
    };
    input.parse::<Option<syn::Token![,]>>()?;
    Ok(MacroBody::Match { value, pattern, guard })
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
            MacroBody::Match { value, pattern, guard } => {
                visitor.visit_expr(value);
                visitor.visit_pat(pattern);
                if let Some(guard) = guard {
                    visitor.visit_expr(guard);
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
    /// An absolute path uses the external-crate namespace even if a local
    /// module has the same name.
    pub(crate) leading_colon: bool,
    /// Whether the path names a type rather than a value (`type_position`).
    pub(crate) type_position: bool,
}

/// Every path in `tokens`, nested groups included. A leading `::` is recorded
/// separately from the path's segments.
pub(crate) fn token_paths(tokens: TokenStream) -> Vec<TokenPath> {
    let mut paths = Vec::new();
    collect_token_paths(tokens, Context::Code, &mut paths);
    paths
}

/// What a group's tokens are.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Context {
    /// Code, where a path is a value unless something says it is a type.
    Code,
    /// A type's tokens: `(A, B)`, `[A; 3]`, `Fn(A)`'s arguments.
    Type,
    /// A struct literal's fields, where `field: value` names a value.
    Literal,
}

/// What the tokens before a path say about it.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Introducer {
    /// A single `:`: a type in a field, parameter or `let`, or a struct
    /// literal's field value.
    Colon,
    /// `->`, `impl`, `dyn`, `for`, `as`, `where`, or a declaration's keyword.
    Type,
    None,
}

/// What comes before the path or group starting at `start`, skipping the
/// `&`, `&mut`, `*const`, `*mut` and lifetimes a type can start with.
fn introducer(trees: &[TokenTree], start: usize) -> Introducer {
    let mut index = start;
    while let Some(before) = index.checked_sub(1) {
        match &trees[before] {
            TokenTree::Punct(punct) if matches!(punct.as_char(), '&' | '*') => index = before,
            TokenTree::Ident(ident) if ident == "mut" || ident == "const" => index = before,
            TokenTree::Ident(_)
                if before >= 1
                    && matches!(&trees[before - 1], TokenTree::Punct(punct) if punct.as_char() == '\'') =>
            {
                index = before - 1;
            }
            _ => break,
        }
    }
    let Some(before) = index.checked_sub(1) else { return Introducer::None };
    match &trees[before] {
        TokenTree::Punct(colon) if colon.as_char() == ':' => {
            let doubled = before >= 1
                && matches!(&trees[before - 1], TokenTree::Punct(punct) if punct.as_char() == ':');
            if doubled {
                Introducer::None
            } else {
                Introducer::Colon
            }
        }
        TokenTree::Punct(arrow) if arrow.as_char() == '>' => {
            let after_minus = before >= 1
                && matches!(&trees[before - 1], TokenTree::Punct(punct) if punct.as_char() == '-');
            if after_minus {
                Introducer::Type
            } else {
                Introducer::None
            }
        }
        TokenTree::Ident(keyword)
            if matches!(
                keyword.to_string().as_str(),
                "impl"
                    | "dyn"
                    | "for"
                    | "as"
                    | "where"
                    | "struct"
                    | "enum"
                    | "union"
                    | "trait"
                    | "type"
                    | "mod"
                    | "use"
                    | "fn"
            ) =>
        {
            Introducer::Type
        }
        _ => Introducer::None,
    }
}

/// `<` opens type arguments after a path or `::`; elsewhere it compares.
fn opens_type_arguments(trees: &[TokenTree], index: usize) -> bool {
    index >= 1
        && match &trees[index - 1] {
            TokenTree::Ident(_) => true,
            TokenTree::Punct(punct) => punct.as_char() == ':',
            _ => false,
        }
}

/// A path names a type or a declaration, not a value, when it follows `->`,
/// `impl`, `dyn`, `for`, `as` or a declaration's keyword. Otherwise a path
/// followed by `(` is a call, unless it names one of the `Fn` traits; and any
/// other path is a type inside type arguments or a group a type opens
/// (`Fn(A)`, `(A, B)` after `:`), or after a single `:` with no struct
/// literal after it, unless it is a struct literal's field value.
fn collect_token_paths(tokens: TokenStream, context: Context, paths: &mut Vec<TokenPath>) {
    let trees = tokens.into_iter().collect::<Vec<_>>();
    let mut type_arguments = 0usize;
    // The index right after the last path, and what a group there is.
    let mut last_path = None;
    let introduces_type = |trees: &[TokenTree], start: usize| match introducer(trees, start) {
        Introducer::Type => true,
        Introducer::Colon => context != Context::Literal,
        Introducer::None => false,
    };
    let mut index = 0;
    while index < trees.len() {
        match &trees[index] {
            // A brace group is a body, or a struct literal after a value's
            // path; never a type.
            TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => {
                let literal = last_path == Some((index, Context::Literal));
                let context = if literal { Context::Literal } else { Context::Code };
                collect_token_paths(group.stream(), context, paths);
                index += 1;
            }
            TokenTree::Group(group) => {
                let opened_by_type = context == Context::Type
                    || type_arguments > 0
                    || last_path == Some((index, Context::Type))
                    || introduces_type(&trees, index);
                let context = if opened_by_type { Context::Type } else { Context::Code };
                collect_token_paths(group.stream(), context, paths);
                index += 1;
            }
            TokenTree::Punct(punct) if punct.as_char() == '<' => {
                if opens_type_arguments(&trees, index) {
                    type_arguments += 1;
                }
                index += 1;
            }
            TokenTree::Punct(punct) if punct.as_char() == '>' => {
                let arrow = index >= 1
                    && matches!(&trees[index - 1], TokenTree::Punct(before) if matches!(before.as_char(), '-' | '='));
                if type_arguments > 0 && !arrow {
                    type_arguments -= 1;
                }
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
                let leading_colon = index >= 2
                    && matches!(&trees[index - 2], TokenTree::Punct(punct) if punct.as_char() == ':')
                    && matches!(&trees[index - 1], TokenTree::Punct(punct) if punct.as_char() == ':');
                let start = if leading_colon { index - 2 } else { index };
                let fn_trait =
                    matches!(segments.last().map(String::as_str), Some("Fn" | "FnMut" | "FnOnce"));
                let call = followed_by == Some(Delimiter::Parenthesis) && !fn_trait;
                let type_position = !after_dot
                    && match introducer(&trees, start) {
                        Introducer::Type => true,
                        _ if call => false,
                        _ if context == Context::Type || type_arguments > 0 => true,
                        Introducer::Colon => {
                            context != Context::Literal && followed_by != Some(Delimiter::Brace)
                        }
                        Introducer::None => false,
                    };
                // A brace after a value's path opens a struct literal, except
                // after a method or field name, or in a condition, where
                // Rust allows no literal.
                let condition = start >= 1
                    && matches!(&trees[start - 1], TokenTree::Ident(keyword)
                        if matches!(keyword.to_string().as_str(), "if" | "while" | "match" | "in"));
                let group = if type_position {
                    Context::Type
                } else if after_dot || condition {
                    Context::Code
                } else {
                    Context::Literal
                };
                last_path = Some((cursor, group));
                paths.push(TokenPath {
                    segments,
                    span: ident.span(),
                    followed_by,
                    after_dot,
                    receiver,
                    leading_colon,
                    type_position,
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
