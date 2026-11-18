//! The resolver's scopes, as `NameResolver::collect` reads them from the
//! files: each module's and block's definitions, imports and globs, and each
//! `macro_rules!`.

use std::collections::BTreeMap;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use super::{
    position, Binding, Glob, ItemPath, MacroDefinition, NameResolver, Namespace, Region, Scope,
    Visibility,
};
use crate::macros::parse_macro_body;
use crate::syntax::path_names;

pub(super) struct Collector<'a> {
    pub(super) resolver: &'a mut NameResolver,
    pub(super) file: &'a str,
    pub(super) scope: usize,
}

impl Collector<'_> {
    fn scoped(&mut self, span: Span, visit: impl FnOnce(&mut Self)) {
        let parent = self.scope;
        let mut identity = self.resolver.scopes[parent].identity.clone();
        let start = span.start();
        identity.push(format!("<{}:{}:{}>", self.file, start.line, start.column));
        let scope = self.resolver.scopes.len();
        self.resolver.scopes.push(Scope {
            module: self.resolver.scopes[parent].module.clone(),
            identity,
            parent: Some(parent),
            names: BTreeMap::new(),
            globs: Vec::new(),
            macros: BTreeMap::new(),
        });
        self.resolver.regions.entry(self.file.to_string()).or_default().push(Region {
            start: position(start),
            end: position(span.end()),
            scope,
        });
        self.scope = scope;
        visit(self);
        self.scope = parent;
    }

    /// Who may name what this scope declares with `visibility`: the module
    /// the scope is in for a private one, as for an item in a function's
    /// body, which no glob reaches.
    fn visibility(&self, visibility: &syn::Visibility) -> Visibility {
        let module = &self.resolver.scopes[self.scope].module;
        let restricted = match visibility {
            syn::Visibility::Public(_) => return Vec::new(),
            syn::Visibility::Inherited => return module.clone(),
            syn::Visibility::Restricted(restricted) => path_names(&restricted.path),
        };
        let (base, rest) = match self.resolver.module_prefix(module, &restricted) {
            Ok(Some(prefixed)) => prefixed,
            // A path of the 2015 edition, from the crate's root.
            Ok(None) => (vec![module[0].clone()], restricted.as_slice()),
            // `pub(super)` at a crate's root, which rustc refuses: the item
            // stays private.
            Err(_) => return module.clone(),
        };
        base.into_iter().chain(rest.iter().cloned()).collect()
    }

    fn declare(
        &mut self,
        ident: &syn::Ident,
        namespace: Namespace,
        visibility: Visibility,
    ) -> ItemPath {
        let mut path = self.resolver.scopes[self.scope].identity.clone();
        path.push(ident.to_string());
        self.resolver.scopes[self.scope]
            .names
            .entry(ident.to_string())
            .or_default()
            .push(Binding::Definition(path.clone(), namespace, visibility));
        self.resolver.definitions.insert(
            (self.file.to_string(), position(ident.span().start()), ident.to_string()),
            path.clone(),
        );
        path
    }

    fn import(&mut self, tree: &syn::UseTree, prefix: &mut ItemPath, visibility: &Visibility) {
        match tree {
            syn::UseTree::Path(path) => {
                prefix.push(path.ident.to_string());
                self.import(&path.tree, prefix, visibility);
                prefix.pop();
            }
            syn::UseTree::Group(group) => {
                for tree in &group.items {
                    self.import(tree, prefix, visibility);
                }
            }
            syn::UseTree::Glob(_) => self.resolver.scopes[self.scope]
                .globs
                .push(Glob { path: prefix.clone(), visibility: visibility.clone() }),
            syn::UseTree::Name(name) => {
                let mut path = prefix.clone();
                let local = if name.ident == "self" {
                    path.last().expect("self import prefix").clone()
                } else {
                    path.push(name.ident.to_string());
                    name.ident.to_string()
                };
                self.resolver.scopes[self.scope]
                    .names
                    .entry(local)
                    .or_default()
                    .push(Binding::Import(path, visibility.clone()));
            }
            syn::UseTree::Rename(rename) => {
                let mut path = prefix.clone();
                if rename.ident != "self" {
                    path.push(rename.ident.to_string());
                }
                self.resolver.scopes[self.scope]
                    .names
                    .entry(rename.rename.to_string())
                    .or_default()
                    .push(Binding::Import(path, visibility.clone()));
            }
        }
    }
}

impl<'ast> Visit<'ast> for Collector<'_> {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        let named = match node {
            syn::Item::Fn(item) => Some((&item.sig.ident, &item.vis)),
            syn::Item::Struct(item) => Some((&item.ident, &item.vis)),
            syn::Item::Enum(item) => Some((&item.ident, &item.vis)),
            syn::Item::Union(item) => Some((&item.ident, &item.vis)),
            syn::Item::Type(item) => Some((&item.ident, &item.vis)),
            syn::Item::Trait(item) => Some((&item.ident, &item.vis)),
            syn::Item::Const(item) => Some((&item.ident, &item.vis)),
            syn::Item::Static(item) => Some((&item.ident, &item.vis)),
            _ => None,
        };
        if let Some((ident, visibility)) = named {
            let namespace =
                if matches!(node, syn::Item::Fn(_) | syn::Item::Const(_) | syn::Item::Static(_)) {
                    Namespace::Value
                } else {
                    Namespace::Type
                };
            let visibility = self.visibility(visibility);
            let path = self.declare(ident, namespace, visibility.clone());
            if matches!(node, syn::Item::Struct(_)) {
                self.declare(ident, Namespace::Value, visibility);
            }
            if matches!(
                node,
                syn::Item::Struct(_)
                    | syn::Item::Enum(_)
                    | syn::Item::Union(_)
                    | syn::Item::Type(_)
                    | syn::Item::Trait(_)
            ) {
                self.resolver.types.insert(path.clone());
            }
            if let syn::Item::Enum(item) = node {
                self.resolver
                    .enum_members
                    .entry(path)
                    .or_default()
                    .extend(item.variants.iter().map(|variant| variant.ident.to_string()));
            }
        }
        visit::visit_item(self, node);
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        let mut prefix =
            if node.leading_colon.is_some() { vec![String::new()] } else { Vec::new() };
        let visibility = self.visibility(&node.vis);
        self.import(&node.tree, &mut prefix, &visibility);
    }

    fn visit_item_extern_crate(&mut self, node: &'ast syn::ItemExternCrate) {
        if has_attribute(&node.attrs, "macro_use") {
            let crate_name = self.resolver.scopes[self.scope].module[0].clone();
            self.resolver
                .macro_use_crates
                .entry(crate_name)
                .or_default()
                .push(node.ident.to_string());
        }
        let name = node.rename.as_ref().map_or(&node.ident, |(_, name)| name).to_string();
        let target = if node.ident == "self" {
            self.resolver.scopes[self.scope].module[0].clone()
        } else {
            node.ident.to_string()
        };
        let visibility = self.visibility(&node.vis);
        let binding = Binding::Definition(vec![target], Namespace::Type, visibility);
        self.resolver.scopes[self.scope].names.entry(name).or_default().push(binding);
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        let visibility = self.visibility(&node.vis);
        let scope = self.resolver.local_module(self.scope, &node.ident.to_string(), visibility);
        if has_attribute(&node.attrs, "macro_use") {
            let parent = self.resolver.scopes[self.scope].identity.clone();
            let child = self.resolver.scopes[scope].identity.clone();
            self.resolver.macro_use_modules.entry(parent).or_default().push(child);
        }
        if node.content.is_some() {
            let previous = std::mem::replace(&mut self.scope, scope);
            self.resolver.regions.entry(self.file.to_string()).or_default().push(Region {
                start: position(node.span().start()),
                end: position(node.span().end()),
                scope,
            });
            visit::visit_item_mod(self, node);
            self.scope = previous;
        }
    }

    fn visit_block(&mut self, node: &'ast syn::Block) {
        self.scoped(node.span(), |collector| visit::visit_block(collector, node));
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        self.scoped(node.span(), |collector| visit::visit_item_fn(collector, node));
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        self.scoped(node.span(), |collector| visit::visit_item_impl(collector, node));
    }

    fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
        self.scoped(node.span(), |collector| visit::visit_item_trait(collector, node));
    }

    fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
        self.scoped(node.span(), |collector| visit::visit_item_struct(collector, node));
    }

    fn visit_item_enum(&mut self, node: &'ast syn::ItemEnum) {
        self.scoped(node.span(), |collector| visit::visit_item_enum(collector, node));
    }

    fn visit_item_type(&mut self, node: &'ast syn::ItemType) {
        self.scoped(node.span(), |collector| visit::visit_item_type(collector, node));
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        self.scoped(node.span(), |collector| visit::visit_impl_item_fn(collector, node));
    }

    fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
        self.scoped(node.span(), |collector| visit::visit_trait_item_fn(collector, node));
    }

    fn visit_type_param(&mut self, node: &'ast syn::TypeParam) {
        let visibility = self.resolver.scopes[self.scope].module.clone();
        self.declare(&node.ident, Namespace::Type, visibility);
    }

    fn visit_foreign_item_fn(&mut self, node: &'ast syn::ForeignItemFn) {
        let visibility = self.visibility(&node.vis);
        self.declare(&node.sig.ident, Namespace::Value, visibility);
    }

    /// A `macro_rules!` is in textual scope where it is defined. One with
    /// `#[macro_export]` is also a path at its crate's root, and one at a
    /// module's top level is a path in that module, for a `use` to re-export.
    fn visit_item_macro(&mut self, node: &'ast syn::ItemMacro) {
        let Some(name) = node.ident.as_ref().filter(|_| node.mac.path.is_ident("macro_rules"))
        else {
            visit::visit_item_macro(self, node);
            return;
        };
        let crate_name = self.resolver.scopes[self.scope].module[0].clone();
        let index = self.resolver.macros.len();
        self.resolver.macros.push(MacroDefinition {
            name: name.to_string(),
            file: self.file.to_string(),
            position: position(name.span().start()),
            crate_name: crate_name.clone(),
            definition: node.mac.clone(),
        });
        self.resolver.scopes[self.scope].macros.entry(name.to_string()).or_default().push(index);
        let path = if has_attribute(&node.attrs, "macro_export") {
            let root = self.resolver.module(std::slice::from_ref(&crate_name));
            let path = vec![crate_name, name.to_string()];
            self.resolver.scopes[root]
                .names
                .entry(name.to_string())
                .or_default()
                .push(Binding::Definition(path.clone(), Namespace::Macro, Vec::new()));
            Some(path)
        } else if self.resolver.scopes[self.scope].parent.is_none() {
            // A `macro_rules!` is private to its module, which re-exports
            // it with `use`.
            let visibility = self.resolver.scopes[self.scope].module.clone();
            Some(self.declare(name, Namespace::Macro, visibility))
        } else {
            None
        };
        if let Some(path) = path {
            self.resolver.macro_paths.entry(path).or_default().push(index);
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

fn has_attribute(attrs: &[syn::Attribute], name: &str) -> bool {
    attrs.iter().any(|attribute| attribute.path().is_ident(name))
}
