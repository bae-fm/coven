//! The workspace's own macros, expanded before the rules read the code
//! (Appendix F.6: the checker reads "inside macro calls"). Each invocation of a
//! `macro_rules!` the workspace defines is replaced by the code it writes,
//! wherever it is: among items, in an `impl` or trait, as a statement, an
//! expression, a type or a pattern, and inside another crate's macro whose
//! arguments parse as code. An expansion that invokes a macro is expanded in
//! turn. The definitions themselves are removed: a definition is no code until
//! it is used, and its uses are what the rules read.
//!
//! Which definition an invocation names follows rustc (`NameResolver::resolve_macro`).
//! A macro of a dependency or of the standard library stays as written, and
//! the rules read its arguments as `macros.rs` describes.
//!
//! An invocation the checker cannot expand is a finding, never a guess: what
//! the macro writes would otherwise go unchecked.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::{Delimiter, Ident, Punct, Spacing, Span, TokenStream, TokenTree};
use quote::ToTokens;
use syn::parse::{ParseStream, Parser};
use syn::spanned::Spanned;
use syn::visit_mut::{self, VisitMut};

use crate::finding::Finding;
use crate::macro_rules::{Expansion, MacroRules};
use crate::macros::{parse_macro_body, unparsed_tokens, MacroBody};
use crate::name_resolution::{MacroLookup, NameResolver};
use crate::syntax::RustFile;

/// How many expansions one file may hold; more means a macro that keeps
/// invoking itself, which rustc would stop at its recursion limit too.
const EXPANSIONS_PER_FILE: usize = 100_000;

const REMEDY: &str = "the checker expands the workspace's own macro_rules! to check the code they write; write the invocation so it can, or teach macro_rules.rs the form it uses";

/// The files with the workspace's macros expanded, and each invocation the
/// checker could not expand.
pub(crate) struct Expanded {
    pub(crate) files: Vec<RustFile>,
    pub(crate) findings: Vec<Finding>,
}

pub(crate) fn expand_macros(files: &[RustFile]) -> Expanded {
    let resolver = NameResolver::collect(files);
    let mut rules = BTreeMap::new();
    let mut findings = BTreeSet::new();
    let files = files
        .iter()
        .map(|file| {
            let mut file = file.clone();
            let mut expander = Expander {
                file: &file.relative_path,
                resolver: &resolver,
                rules: &mut rules,
                findings: &mut findings,
                budget: EXPANSIONS_PER_FILE,
            };
            let mut syntax = file.syntax.clone();
            expander.visit_file_mut(&mut syntax);
            file.syntax = syntax;
            file
        })
        .collect();
    Expanded { files, findings: findings.into_iter().collect() }
}

struct Expander<'a> {
    file: &'a str,
    resolver: &'a NameResolver,
    /// Each definition's arms, read once.
    rules: &'a mut BTreeMap<usize, Result<MacroRules, String>>,
    findings: &'a mut BTreeSet<Finding>,
    budget: usize,
}

fn is_definition(mac: &syn::Macro) -> bool {
    mac.path.is_ident("macro_rules")
}

impl Expander<'_> {
    fn report(&mut self, mac: &syn::Macro, reason: impl std::fmt::Display) {
        let name = mac.path.segments.last().map_or_else(String::new, |s| s.ident.to_string());
        self.findings.insert(Finding::new(
            self.file,
            mac.path.span().start().line,
            format!("cannot expand {name}!: {reason}"),
            REMEDY,
        ));
    }

    /// The tokens an invocation of a workspace macro writes. `None` for a
    /// macro another crate defines, and for one the checker cannot expand,
    /// which it reports.
    fn expansion(&mut self, mac: &syn::Macro) -> Option<TokenStream> {
        let index = match self.resolver.resolve_macro(self.file, &mac.path) {
            MacroLookup::External => return None,
            MacroLookup::Unresolved(error) => {
                self.report(mac, format!("no definition answers its name: {error}"));
                return None;
            }
            MacroLookup::Workspace(index) => index,
        };
        if self.budget == 0 {
            self.report(mac, "the file holds more expansions than a macro can write without invoking itself without end");
            return None;
        }
        self.budget -= 1;
        let definition = self.resolver.macro_definition(index);
        let rules = self.rules.entry(index).or_insert_with(|| {
            MacroRules::parse(definition.definition.tokens.clone())
                .map_err(|error| error.to_string())
        });
        let rules = match rules {
            Ok(rules) => rules,
            Err(error) => {
                let error = error.clone();
                self.report(mac, error);
                return None;
            }
        };
        let span = mac.path.segments.last().map_or_else(|| mac.path.span(), |s| s.ident.span());
        let crate_path = if self.resolver.crate_at(self.file, span) == definition.crate_name {
            TokenStream::from(TokenTree::Ident(Ident::new("crate", span)))
        } else {
            let mut first = Punct::new(':', Spacing::Joint);
            first.set_span(span);
            let mut second = Punct::new(':', Spacing::Alone);
            second.set_span(span);
            [
                TokenTree::Punct(first),
                TokenTree::Punct(second),
                TokenTree::Ident(Ident::new(&definition.crate_name, span)),
            ]
            .into_iter()
            .collect()
        };
        match rules.expand(mac.tokens.clone(), &Expansion { crate_path, span }) {
            Ok(tokens) => Some(tokens),
            Err(error) => {
                self.report(mac, error);
                None
            }
        }
    }

    /// Parses an expansion as `what`, reporting it when it does not parse.
    fn parsed<T>(
        &mut self,
        mac: &syn::Macro,
        tokens: TokenStream,
        what: &str,
        parser: impl Parser<Output = T>,
    ) -> Option<T> {
        match parser.parse2(tokens) {
            Ok(parsed) => Some(parsed),
            Err(error) => {
                self.report(mac, format!("what it writes does not parse as {what}: {error}"));
                None
            }
        }
    }

    /// Replaces each item-position invocation with the items it writes, and
    /// drops each definition.
    fn expand_items(&mut self, items: &mut Vec<syn::Item>) {
        let mut index = 0;
        while index < items.len() {
            if let syn::Item::Macro(item) = &items[index] {
                if is_definition(&item.mac) {
                    items.remove(index);
                    continue;
                }
                let mac = item.mac.clone();
                if let Some(expanded) = self
                    .expansion(&mac)
                    .and_then(|tokens| self.parsed(&mac, tokens, "items", parse_all::<syn::Item>))
                {
                    items.splice(index..=index, expanded);
                    continue;
                }
            }
            index += 1;
        }
    }

    fn expand_impl_items(&mut self, items: &mut Vec<syn::ImplItem>) {
        let mut index = 0;
        while index < items.len() {
            if let syn::ImplItem::Macro(item) = &items[index] {
                let mac = item.mac.clone();
                if let Some(expanded) = self.expansion(&mac).and_then(|tokens| {
                    self.parsed(&mac, tokens, "associated items", parse_all::<syn::ImplItem>)
                }) {
                    items.splice(index..=index, expanded);
                    continue;
                }
            }
            index += 1;
        }
    }

    fn expand_trait_items(&mut self, items: &mut Vec<syn::TraitItem>) {
        let mut index = 0;
        while index < items.len() {
            if let syn::TraitItem::Macro(item) = &items[index] {
                let mac = item.mac.clone();
                if let Some(expanded) = self.expansion(&mac).and_then(|tokens| {
                    self.parsed(&mac, tokens, "trait items", parse_all::<syn::TraitItem>)
                }) {
                    items.splice(index..=index, expanded);
                    continue;
                }
            }
            index += 1;
        }
    }

    fn expand_statements(&mut self, statements: &mut Vec<syn::Stmt>) {
        let mut index = 0;
        while index < statements.len() {
            let mac = match &statements[index] {
                syn::Stmt::Item(syn::Item::Macro(item)) if is_definition(&item.mac) => {
                    statements.remove(index);
                    continue;
                }
                syn::Stmt::Item(syn::Item::Macro(item)) => Some(item.mac.clone()),
                syn::Stmt::Macro(statement) => Some(statement.mac.clone()),
                _ => None,
            };
            if let Some(mac) = mac {
                if let Some(expanded) = self.expansion(&mac).and_then(|tokens| {
                    self.parsed(&mac, tokens, "statements", syn::Block::parse_within)
                }) {
                    statements.splice(index..=index, expanded);
                    continue;
                }
            }
            index += 1;
        }
    }

    /// Expands the invocations inside another crate's macro whose arguments
    /// parse as code, writing the arguments back as tokens. One whose
    /// arguments do not parse cannot be read, so an invocation of a workspace
    /// macro inside it is reported.
    fn expand_inside(&mut self, node: &mut syn::Macro) {
        let Some(mut body) = parse_macro_body(node) else {
            for (name, span) in invocations_in(unparsed_tokens(node)) {
                if self.resolver.names_a_macro(&name) {
                    self.findings.insert(Finding::new(
                        self.file,
                        span.start().line,
                        format!(
                            "cannot expand {name}!: it is inside the arguments of {}!, which do not parse as code",
                            node.path.segments.last().map_or_else(String::new, |s| s.ident.to_string())
                        ),
                        REMEDY,
                    ));
                }
            }
            return;
        };
        let before = self.budget;
        match &mut body {
            MacroBody::Expressions(expressions) => {
                for expression in expressions.iter_mut() {
                    self.visit_expr_mut(expression);
                }
            }
            MacroBody::Statements(statements) => {
                self.expand_statements(statements);
                for statement in statements.iter_mut() {
                    self.visit_stmt_mut(statement);
                }
            }
            MacroBody::Match { value, pattern, guard } => {
                self.visit_expr_mut(value);
                self.visit_pat_mut(pattern);
                if let Some(guard) = guard {
                    self.visit_expr_mut(guard);
                }
            }
        }
        if self.budget != before {
            node.tokens = body_tokens(&body);
        }
    }
}

/// The tokens of a macro's arguments, as `parse_macro_body` read them.
fn body_tokens(body: &MacroBody) -> TokenStream {
    let mut tokens = TokenStream::new();
    match body {
        MacroBody::Expressions(expressions) => expressions.to_tokens(&mut tokens),
        MacroBody::Statements(statements) => {
            for statement in statements {
                statement.to_tokens(&mut tokens);
            }
        }
        MacroBody::Match { value, pattern, guard } => {
            value.to_tokens(&mut tokens);
            <syn::Token![,]>::default().to_tokens(&mut tokens);
            pattern.to_tokens(&mut tokens);
            if let Some(guard) = guard {
                <syn::Token![if]>::default().to_tokens(&mut tokens);
                guard.to_tokens(&mut tokens);
            }
        }
    }
    tokens
}

/// Each `name!(…)` in tokens a parser could not read, nested groups included.
fn invocations_in(tokens: TokenStream) -> Vec<(String, Span)> {
    let trees = tokens.into_iter().collect::<Vec<_>>();
    let mut invocations = Vec::new();
    for (index, tree) in trees.iter().enumerate() {
        match tree {
            TokenTree::Group(group) => invocations.extend(invocations_in(group.stream())),
            TokenTree::Ident(name) => {
                let bang = matches!(trees.get(index + 1), Some(TokenTree::Punct(punct)) if punct.as_char() == '!');
                let arguments = matches!(trees.get(index + 2), Some(TokenTree::Group(group)) if group.delimiter() != Delimiter::None);
                if bang && arguments {
                    invocations.push((name.to_string(), name.span()));
                }
            }
            _ => {}
        }
    }
    invocations
}

/// Every `T` in the stream, to its end.
fn parse_all<T: syn::parse::Parse>(input: ParseStream<'_>) -> syn::Result<Vec<T>> {
    let mut parsed = Vec::new();
    while !input.is_empty() {
        parsed.push(input.parse()?);
    }
    Ok(parsed)
}

impl VisitMut for Expander<'_> {
    fn visit_file_mut(&mut self, node: &mut syn::File) {
        self.expand_items(&mut node.items);
        visit_mut::visit_file_mut(self, node);
    }

    fn visit_item_mod_mut(&mut self, node: &mut syn::ItemMod) {
        if let Some((_, items)) = &mut node.content {
            self.expand_items(items);
        }
        visit_mut::visit_item_mod_mut(self, node);
    }

    fn visit_item_impl_mut(&mut self, node: &mut syn::ItemImpl) {
        self.expand_impl_items(&mut node.items);
        visit_mut::visit_item_impl_mut(self, node);
    }

    fn visit_item_trait_mut(&mut self, node: &mut syn::ItemTrait) {
        self.expand_trait_items(&mut node.items);
        visit_mut::visit_item_trait_mut(self, node);
    }

    fn visit_block_mut(&mut self, node: &mut syn::Block) {
        self.expand_statements(&mut node.stmts);
        visit_mut::visit_block_mut(self, node);
    }

    fn visit_expr_mut(&mut self, node: &mut syn::Expr) {
        if let syn::Expr::Macro(expression) = node {
            let mac = expression.mac.clone();
            if let Some(tokens) = self.expansion(&mac) {
                let parser = |input: ParseStream<'_>| {
                    let fork = input.fork();
                    if let Ok(expression) = fork.parse::<syn::Expr>() {
                        if fork.is_empty() {
                            input.parse::<TokenStream>()?;
                            return Ok(expression);
                        }
                    }
                    let statements = syn::Block::parse_within(input)?;
                    Ok(syn::Expr::Block(syn::ExprBlock {
                        attrs: Vec::new(),
                        label: None,
                        block: syn::Block { brace_token: Default::default(), stmts: statements },
                    }))
                };
                if let Some(expanded) = self.parsed(&mac, tokens, "an expression", parser) {
                    *node = expanded;
                }
            }
        }
        visit_mut::visit_expr_mut(self, node);
    }

    fn visit_type_mut(&mut self, node: &mut syn::Type) {
        if let syn::Type::Macro(ty) = node {
            let mac = ty.mac.clone();
            if let Some(expanded) = self.expansion(&mac).and_then(|tokens| {
                self.parsed(&mac, tokens, "a type", |input: ParseStream<'_>| {
                    input.parse::<syn::Type>()
                })
            }) {
                *node = expanded;
            }
        }
        visit_mut::visit_type_mut(self, node);
    }

    fn visit_pat_mut(&mut self, node: &mut syn::Pat) {
        if let syn::Pat::Macro(pattern) = node {
            let mac = pattern.mac.clone();
            if let Some(expanded) = self.expansion(&mac).and_then(|tokens| {
                self.parsed(&mac, tokens, "a pattern", syn::Pat::parse_multi_with_leading_vert)
            }) {
                *node = expanded;
            }
        }
        visit_mut::visit_pat_mut(self, node);
    }

    /// A macro still here is another crate's, or one the checker could not
    /// expand.
    fn visit_macro_mut(&mut self, node: &mut syn::Macro) {
        if !is_definition(node) {
            self.expand_inside(node);
        }
    }
}

#[cfg(test)]
#[path = "macro_expansion_tests.rs"]
mod tests;
