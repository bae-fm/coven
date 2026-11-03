//! Test code, defined once for every rule (Appendix F.3, F.6): a file the test
//! layout names as tests or that only test modules reach
//! (`RustFile::is_test_source`), a file whose root is `#![cfg(test)]`, and
//! every item, statement, field, variant, match arm and struct-literal field
//! that compiles only into tests (`syntax::is_test_only`).
//!
//! The rules of F.1 to F.3 read the code that remains: test code may assemble
//! its own graphs from raw material, and nothing outside it can reach what it
//! declares. The conventions of F.5 read every file, tests included, since
//! where tests live is their subject, and the database's domains read the SQL
//! in test code, since a domain's tests live with it.

use syn::punctuated::Punctuated;
use syn::visit_mut::{self, VisitMut};

use crate::syntax::{is_test_only, RustFile};

/// The crates' files without their test code.
pub(crate) fn production_code(files: &[RustFile]) -> Vec<RustFile> {
    files
        .iter()
        .filter(|file| !file.is_test_source() && !is_test_only(&file.syntax.attrs))
        .map(|file| {
            let mut file = file.clone();
            TestCode.visit_file_mut(&mut file.syntax);
            file
        })
        .collect()
}

/// The crates' test code: each test file whole, and from every other file
/// its test-only items, impl items among them.
pub(crate) fn test_code(files: &[RustFile]) -> Vec<RustFile> {
    files
        .iter()
        .map(|file| {
            let mut file = file.clone();
            if !file.is_test_source() && !is_test_only(&file.syntax.attrs) {
                file.syntax.items = test_items(&file.syntax.items);
            }
            file
        })
        .collect()
}

/// The test-only items among `items`, with those of inline modules and impls
/// that are not test-only themselves.
fn test_items(items: &[syn::Item]) -> Vec<syn::Item> {
    items
        .iter()
        .filter_map(|item| {
            if is_test_only(item_attrs(item)) {
                return Some(item.clone());
            }
            match item {
                syn::Item::Mod(module) => {
                    let (brace, items) = module.content.as_ref()?;
                    let items = test_items(items);
                    (!items.is_empty()).then(|| {
                        syn::Item::Mod(syn::ItemMod {
                            content: Some((*brace, items)),
                            ..module.clone()
                        })
                    })
                }
                syn::Item::Impl(implementation) => {
                    let items = implementation
                        .items
                        .iter()
                        .filter(|item| is_test_only(impl_item_attrs(item)))
                        .cloned()
                        .collect::<Vec<_>>();
                    (!items.is_empty())
                        .then(|| syn::Item::Impl(syn::ItemImpl { items, ..implementation.clone() }))
                }
                _ => None,
            }
        })
        .collect()
}

/// Removes every test-only part of a syntax tree.
struct TestCode;

impl VisitMut for TestCode {
    fn visit_file_mut(&mut self, node: &mut syn::File) {
        node.items.retain(|item| !is_test_only(item_attrs(item)));
        visit_mut::visit_file_mut(self, node);
    }

    fn visit_item_mod_mut(&mut self, node: &mut syn::ItemMod) {
        if let Some((_, items)) = &mut node.content {
            items.retain(|item| !is_test_only(item_attrs(item)));
        }
        visit_mut::visit_item_mod_mut(self, node);
    }

    fn visit_item_impl_mut(&mut self, node: &mut syn::ItemImpl) {
        node.items.retain(|item| !is_test_only(impl_item_attrs(item)));
        visit_mut::visit_item_impl_mut(self, node);
    }

    fn visit_item_trait_mut(&mut self, node: &mut syn::ItemTrait) {
        node.items.retain(|item| !is_test_only(trait_item_attrs(item)));
        visit_mut::visit_item_trait_mut(self, node);
    }

    fn visit_item_foreign_mod_mut(&mut self, node: &mut syn::ItemForeignMod) {
        node.items.retain(|item| !is_test_only(foreign_item_attrs(item)));
        visit_mut::visit_item_foreign_mod_mut(self, node);
    }

    fn visit_item_enum_mut(&mut self, node: &mut syn::ItemEnum) {
        retain(&mut node.variants, |variant| !is_test_only(&variant.attrs));
        visit_mut::visit_item_enum_mut(self, node);
    }

    fn visit_fields_named_mut(&mut self, node: &mut syn::FieldsNamed) {
        retain(&mut node.named, |field| !is_test_only(&field.attrs));
        visit_mut::visit_fields_named_mut(self, node);
    }

    fn visit_fields_unnamed_mut(&mut self, node: &mut syn::FieldsUnnamed) {
        retain(&mut node.unnamed, |field| !is_test_only(&field.attrs));
        visit_mut::visit_fields_unnamed_mut(self, node);
    }

    fn visit_block_mut(&mut self, node: &mut syn::Block) {
        node.stmts.retain(|statement| !is_test_only(stmt_attrs(statement)));
        visit_mut::visit_block_mut(self, node);
    }

    fn visit_expr_match_mut(&mut self, node: &mut syn::ExprMatch) {
        node.arms.retain(|arm| !is_test_only(&arm.attrs));
        visit_mut::visit_expr_match_mut(self, node);
    }

    fn visit_expr_struct_mut(&mut self, node: &mut syn::ExprStruct) {
        retain(&mut node.fields, |field| !is_test_only(&field.attrs));
        visit_mut::visit_expr_struct_mut(self, node);
    }
}

fn retain<T, P>(list: &mut Punctuated<T, P>, keep: impl Fn(&T) -> bool) {
    *list = std::mem::take(list).into_pairs().filter(|pair| keep(pair.value())).collect();
}

fn item_attrs(item: &syn::Item) -> &[syn::Attribute] {
    match item {
        syn::Item::Const(item) => &item.attrs,
        syn::Item::Enum(item) => &item.attrs,
        syn::Item::ExternCrate(item) => &item.attrs,
        syn::Item::Fn(item) => &item.attrs,
        syn::Item::ForeignMod(item) => &item.attrs,
        syn::Item::Impl(item) => &item.attrs,
        syn::Item::Macro(item) => &item.attrs,
        syn::Item::Mod(item) => &item.attrs,
        syn::Item::Static(item) => &item.attrs,
        syn::Item::Struct(item) => &item.attrs,
        syn::Item::Trait(item) => &item.attrs,
        syn::Item::TraitAlias(item) => &item.attrs,
        syn::Item::Type(item) => &item.attrs,
        syn::Item::Union(item) => &item.attrs,
        syn::Item::Use(item) => &item.attrs,
        _ => &[],
    }
}

fn impl_item_attrs(item: &syn::ImplItem) -> &[syn::Attribute] {
    match item {
        syn::ImplItem::Const(item) => &item.attrs,
        syn::ImplItem::Fn(item) => &item.attrs,
        syn::ImplItem::Type(item) => &item.attrs,
        syn::ImplItem::Macro(item) => &item.attrs,
        _ => &[],
    }
}

fn trait_item_attrs(item: &syn::TraitItem) -> &[syn::Attribute] {
    match item {
        syn::TraitItem::Const(item) => &item.attrs,
        syn::TraitItem::Fn(item) => &item.attrs,
        syn::TraitItem::Type(item) => &item.attrs,
        syn::TraitItem::Macro(item) => &item.attrs,
        _ => &[],
    }
}

fn foreign_item_attrs(item: &syn::ForeignItem) -> &[syn::Attribute] {
    match item {
        syn::ForeignItem::Fn(item) => &item.attrs,
        syn::ForeignItem::Static(item) => &item.attrs,
        syn::ForeignItem::Type(item) => &item.attrs,
        syn::ForeignItem::Macro(item) => &item.attrs,
        _ => &[],
    }
}

fn stmt_attrs(statement: &syn::Stmt) -> &[syn::Attribute] {
    match statement {
        syn::Stmt::Local(local) => &local.attrs,
        syn::Stmt::Item(item) => item_attrs(item),
        syn::Stmt::Expr(expr, _) => expr_attrs(expr),
        syn::Stmt::Macro(statement) => &statement.attrs,
    }
}

/// The outer attributes of an expression statement, as `#[cfg(test)] check();`.
fn expr_attrs(expr: &syn::Expr) -> &[syn::Attribute] {
    match expr {
        syn::Expr::Array(expr) => &expr.attrs,
        syn::Expr::Assign(expr) => &expr.attrs,
        syn::Expr::Async(expr) => &expr.attrs,
        syn::Expr::Await(expr) => &expr.attrs,
        syn::Expr::Binary(expr) => &expr.attrs,
        syn::Expr::Block(expr) => &expr.attrs,
        syn::Expr::Break(expr) => &expr.attrs,
        syn::Expr::Call(expr) => &expr.attrs,
        syn::Expr::Cast(expr) => &expr.attrs,
        syn::Expr::Closure(expr) => &expr.attrs,
        syn::Expr::Const(expr) => &expr.attrs,
        syn::Expr::Continue(expr) => &expr.attrs,
        syn::Expr::Field(expr) => &expr.attrs,
        syn::Expr::ForLoop(expr) => &expr.attrs,
        syn::Expr::Group(expr) => &expr.attrs,
        syn::Expr::If(expr) => &expr.attrs,
        syn::Expr::Index(expr) => &expr.attrs,
        syn::Expr::Infer(expr) => &expr.attrs,
        syn::Expr::Let(expr) => &expr.attrs,
        syn::Expr::Lit(expr) => &expr.attrs,
        syn::Expr::Loop(expr) => &expr.attrs,
        syn::Expr::Macro(expr) => &expr.attrs,
        syn::Expr::Match(expr) => &expr.attrs,
        syn::Expr::MethodCall(expr) => &expr.attrs,
        syn::Expr::Paren(expr) => &expr.attrs,
        syn::Expr::Path(expr) => &expr.attrs,
        syn::Expr::Range(expr) => &expr.attrs,
        syn::Expr::RawAddr(expr) => &expr.attrs,
        syn::Expr::Reference(expr) => &expr.attrs,
        syn::Expr::Repeat(expr) => &expr.attrs,
        syn::Expr::Return(expr) => &expr.attrs,
        syn::Expr::Struct(expr) => &expr.attrs,
        syn::Expr::Try(expr) => &expr.attrs,
        syn::Expr::TryBlock(expr) => &expr.attrs,
        syn::Expr::Tuple(expr) => &expr.attrs,
        syn::Expr::Unary(expr) => &expr.attrs,
        syn::Expr::Unsafe(expr) => &expr.attrs,
        syn::Expr::While(expr) => &expr.attrs,
        syn::Expr::Yield(expr) => &expr.attrs,
        _ => &[],
    }
}

#[cfg(test)]
#[path = "test_code_tests.rs"]
mod tests;
