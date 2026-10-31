//! Resolve source names in their module and lexical scope before inspecting
//! a definition. External paths retain their identity; they never borrow a
//! workspace definition with the same final identifier.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::parse_macro_body;
use crate::module_sources::module_sources;
use crate::syntax::{is_test_only, path_names, RustFile};

pub(crate) type ItemPath = Vec<String>;
type Position = (usize, usize);

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
enum Namespace {
    Type,
    Value,
}

#[derive(Clone)]
enum Binding {
    Definition(ItemPath, Namespace),
    Import(ItemPath),
}

struct Scope {
    module: ItemPath,
    identity: ItemPath,
    parent: Option<usize>,
    names: BTreeMap<String, Vec<Binding>>,
    globs: Vec<ItemPath>,
    macros: BTreeMap<String, syn::Macro>,
}

struct Region {
    start: Position,
    end: Position,
    scope: usize,
}

pub(crate) struct NameResolver {
    scopes: Vec<Scope>,
    modules: BTreeMap<ItemPath, usize>,
    module_parents: BTreeMap<ItemPath, ItemPath>,
    regions: BTreeMap<String, Vec<Region>>,
    definitions: BTreeMap<(String, Position), ItemPath>,
    pub(crate) types: BTreeSet<ItemPath>,
    enum_members: BTreeMap<ItemPath, BTreeSet<String>>,
    extern_crates: BTreeMap<String, BTreeMap<String, String>>,
    prelude: usize,
    expanded: BTreeMap<(String, Position, ItemPath, String), syn::File>,
}

#[derive(Debug)]
pub(crate) enum ResolutionError {
    Missing,
    WrongNamespace,
    Ambiguous(BTreeSet<ItemPath>),
    ImportCycle,
}

impl std::fmt::Display for ResolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => write!(f, "no definition or import in the caller's scope"),
            Self::WrongNamespace => write!(f, "the name exists in a different Rust namespace"),
            Self::Ambiguous(paths) => write!(
                f,
                "ambiguous paths: {}",
                paths.iter().map(|path| path.join("::")).collect::<Vec<_>>().join(", ")
            ),
            Self::ImportCycle => write!(f, "cyclic imports"),
        }
    }
}

impl NameResolver {
    pub(crate) fn collect(files: &[RustFile]) -> Self {
        let modules = module_sources(files);
        let mut resolver = Self {
            scopes: Vec::new(),
            modules: BTreeMap::new(),
            module_parents: BTreeMap::new(),
            regions: BTreeMap::new(),
            definitions: BTreeMap::new(),
            types: BTreeSet::new(),
            enum_members: BTreeMap::new(),
            extern_crates: BTreeMap::new(),
            prelude: 0,
            expanded: BTreeMap::new(),
        };
        let prelude = resolver.module(&["<prelude>".into()]);
        resolver.prelude = prelude;
        let syntax = syn::parse_file(crate::rust_prelude::IMPORTS).expect("Rust prelude imports");
        Collector { resolver: &mut resolver, file: "<prelude>", scope: prelude }
            .visit_file(&syntax);
        for module in modules.values() {
            for end in 1..=module.len() {
                resolver.module(&module[..end]);
            }
        }
        for file in files {
            let Some(module) = modules.get(&file.relative_path) else {
                continue;
            };
            resolver
                .extern_crates
                .entry(module[0].clone())
                .or_default()
                .extend(file.extern_crates.clone());
            let scope = resolver.module(module);
            resolver.regions.entry(file.relative_path.clone()).or_default().push(Region {
                start: (0, 0),
                end: (usize::MAX, usize::MAX),
                scope,
            });
            Collector { resolver: &mut resolver, file: &file.relative_path, scope }
                .visit_file(&file.syntax);
        }
        resolver
    }

    pub(crate) fn expanded_macro(&self, file: &str, node: &syn::Macro) -> Option<&syn::File> {
        self.expanded.get(&macro_key(file, node))
    }

    /// A trait can be brought into scope under an alias, through a glob, or
    /// anonymously. Failed lookups of other names do not make it visible.
    pub(crate) fn path_in_scope(&self, file: &str, span: Span, path: &ItemPath) -> bool {
        let start = self.scope_at(file, span);
        let mut scope = Some(start);
        let mut names = BTreeSet::from([path.last().expect("trait name").clone()]);
        let mut visited = BTreeSet::new();
        while let Some(current) = scope {
            self.scope_names(current, &mut names, &mut visited);
            if self.scopes[current].names.get("_").into_iter().flatten().any(
                |binding| match binding {
                    Binding::Import(import) => self
                        .resolve_in(current, import, Namespace::Type, &mut BTreeSet::new())
                        .is_ok_and(|resolved| resolved == *path),
                    Binding::Definition(_, _) => false,
                },
            ) {
                return true;
            }
            scope = self.scopes[current].parent;
        }
        self.scope_names(self.prelude, &mut names, &mut visited);
        names.iter().filter(|name| name.as_str() != "_").any(|name| {
            self.lookup(start, name, true, Namespace::Type, &mut BTreeSet::new())
                .is_ok_and(|resolved| resolved == *path)
        })
    }

    fn scope_names(
        &self,
        scope: usize,
        names: &mut BTreeSet<String>,
        visited: &mut BTreeSet<usize>,
    ) {
        if !visited.insert(scope) {
            return;
        }
        names.extend(self.scopes[scope].names.keys().cloned());
        for glob in &self.scopes[scope].globs {
            // Only indexed modules can contribute additional alias names.
            // Whether any collected name resolves is checked at the caller.
            if let Ok(path) = self.resolve_in(scope, glob, Namespace::Type, &mut BTreeSet::new()) {
                if let Some(module) = self.modules.get(&path) {
                    self.scope_names(*module, names, visited);
                }
            }
        }
    }

    fn module(&mut self, path: &[String]) -> usize {
        if let Some(scope) = self.modules.get(path) {
            return *scope;
        }
        let scope = self.scopes.len();
        self.scopes.push(Scope {
            module: path.to_vec(),
            identity: path.to_vec(),
            parent: None,
            names: BTreeMap::new(),
            globs: Vec::new(),
            macros: BTreeMap::new(),
        });
        self.modules.insert(path.to_vec(), scope);
        if path.len() > 1 {
            let parent = self.module(&path[..path.len() - 1]);
            self.module_parents.insert(path.to_vec(), path[..path.len() - 1].to_vec());
            self.scopes[parent]
                .names
                .entry(path.last().expect("module name").clone())
                .or_default()
                .push(Binding::Definition(path.to_vec(), Namespace::Type));
        }
        scope
    }

    fn local_module(&mut self, parent: usize, name: &str) -> usize {
        let mut path = self.scopes[parent].identity.clone();
        path.push(name.to_string());
        if self.scopes[parent].parent.is_none() {
            return self.module(&path);
        }
        let scope = self.scopes.len();
        self.scopes.push(Scope {
            module: path.clone(),
            identity: path.clone(),
            parent: None,
            names: BTreeMap::new(),
            globs: Vec::new(),
            macros: BTreeMap::new(),
        });
        self.modules.insert(path.clone(), scope);
        self.module_parents.insert(path.clone(), self.scopes[parent].module.clone());
        self.scopes[parent]
            .names
            .entry(name.to_string())
            .or_default()
            .push(Binding::Definition(path, Namespace::Type));
        scope
    }

    fn scope_at(&self, file: &str, span: Span) -> usize {
        let position = position(span.start());
        self.regions[file]
            .iter()
            .rev()
            .find(|region| region.start <= position && position <= region.end)
            .expect("file scope")
            .scope
    }

    pub(crate) fn definition(&self, file: &str, ident: &syn::Ident) -> ItemPath {
        self.definitions[&(file.to_string(), position(ident.span().start()))].clone()
    }

    pub(crate) fn resolve(
        &self,
        file: &str,
        span: Span,
        path: &[String],
    ) -> Result<ItemPath, ResolutionError> {
        self.resolve_in(self.scope_at(file, span), path, Namespace::Value, &mut BTreeSet::new())
    }

    pub(crate) fn resolve_type(
        &self,
        file: &str,
        span: Span,
        path: &[String],
    ) -> Result<ItemPath, ResolutionError> {
        self.resolve_in(self.scope_at(file, span), path, Namespace::Type, &mut BTreeSet::new())
    }

    fn resolve_in(
        &self,
        scope: usize,
        path: &[String],
        namespace: Namespace,
        visiting: &mut BTreeSet<(usize, String, bool, Namespace)>,
    ) -> Result<ItemPath, ResolutionError> {
        let Some((first, rest)) = path.split_first() else {
            return Err(ResolutionError::Missing);
        };
        let module = &self.scopes[scope].module;
        let (base, rest) = match first.as_str() {
            "" => {
                let (root, rest) = rest.split_first().ok_or(ResolutionError::Missing)?;
                (self.external(module, root).ok_or(ResolutionError::Missing)?, rest)
            }
            "crate" => (vec![module[0].clone()], rest),
            "self" => (module.clone(), rest),
            "super" => {
                let count = path.iter().take_while(|part| part.as_str() == "super").count();
                let mut parent = module;
                for _ in 0..count {
                    parent = self.module_parents.get(parent).ok_or(ResolutionError::Missing)?;
                }
                (parent.clone(), &path[count..])
            }
            _ => (
                self.lookup(
                    scope,
                    first,
                    true,
                    if rest.is_empty() { namespace } else { Namespace::Type },
                    visiting,
                )?,
                rest,
            ),
        };
        self.descend(base, rest, namespace, visiting)
    }

    fn descend(
        &self,
        mut base: ItemPath,
        rest: &[String],
        namespace: Namespace,
        visiting: &mut BTreeSet<(usize, String, bool, Namespace)>,
    ) -> Result<ItemPath, ResolutionError> {
        for (index, name) in rest.iter().enumerate() {
            if let Some(scope) = self.modules.get(&base) {
                base = self.lookup(
                    *scope,
                    name,
                    false,
                    if index + 1 == rest.len() { namespace } else { Namespace::Type },
                    visiting,
                )?;
            } else {
                base.push(name.clone());
            }
        }
        Ok(base)
    }

    fn lookup(
        &self,
        scope: usize,
        name: &str,
        lexical: bool,
        namespace: Namespace,
        visiting: &mut BTreeSet<(usize, String, bool, Namespace)>,
    ) -> Result<ItemPath, ResolutionError> {
        let key = (scope, name.to_string(), lexical, namespace);
        if !visiting.insert(key.clone()) {
            let definitions = self.scopes[scope]
                .names
                .get(name)
                .into_iter()
                .flatten()
                .filter_map(|binding| match binding {
                    Binding::Definition(path, kind) if *kind == namespace => Some(path.clone()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            return if definitions.is_empty() {
                Err(ResolutionError::ImportCycle)
            } else {
                unique(definitions)
            };
        }
        let result = self.lookup_inner(scope, name, lexical, namespace, visiting);
        visiting.remove(&key);
        result
    }

    fn lookup_inner(
        &self,
        scope: usize,
        name: &str,
        lexical: bool,
        namespace: Namespace,
        visiting: &mut BTreeSet<(usize, String, bool, Namespace)>,
    ) -> Result<ItemPath, ResolutionError> {
        let current = &self.scopes[scope];
        if let Some(bindings) = current.names.get(name) {
            let mut paths = BTreeSet::new();
            for binding in bindings {
                match binding {
                    Binding::Definition(path, kind) if *kind == namespace => {
                        paths.insert(path.clone());
                    }
                    Binding::Definition(_, _) => {}
                    Binding::Import(path) => {
                        match self.resolve_in(scope, path, namespace, visiting) {
                            Ok(path) => {
                                paths.insert(path);
                            }
                            Err(ResolutionError::WrongNamespace) => {}
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            if !paths.is_empty() {
                return unique(paths);
            }
        }
        let missing = if current.names.contains_key(name) {
            ResolutionError::WrongNamespace
        } else {
            ResolutionError::Missing
        };
        let mut paths = BTreeSet::new();
        for glob in &current.globs {
            let target = match self.resolve_in(scope, glob, Namespace::Type, visiting) {
                Ok(target) => target,
                Err(ResolutionError::ImportCycle) => continue,
                Err(error) => return Err(error),
            };
            if let Some(members) = self.enum_members.get(&target) {
                if members.contains(name) {
                    let mut variant = target;
                    variant.push(name.to_string());
                    paths.insert(variant);
                }
                continue;
            }
            match self.descend(target, &[name.to_string()], namespace, visiting) {
                Ok(path) => {
                    paths.insert(path);
                }
                // A glob's absent name is not a failed call. Another import
                // may supply it; multiple supplied definitions are ambiguous.
                Err(
                    ResolutionError::Missing
                    | ResolutionError::WrongNamespace
                    | ResolutionError::ImportCycle,
                ) => {}
                Err(error) => return Err(error),
            }
        }
        if !paths.is_empty() {
            return unique(paths);
        }
        if !lexical {
            return Err(missing);
        }
        if let Some(parent) = current.parent {
            return self.lookup(parent, name, true, namespace, visiting);
        }
        if namespace == Namespace::Type {
            if let Some(path) = self.external(&current.module, name) {
                return Ok(path);
            }
        }
        if scope != self.prelude {
            return match self.lookup(self.prelude, name, false, namespace, visiting) {
                Err(ResolutionError::Missing | ResolutionError::WrongNamespace) => Err(missing),
                result => result,
            };
        }
        Err(missing)
    }

    fn external(&self, module: &[String], name: &str) -> Option<ItemPath> {
        if let Some(target) = self.extern_crates.get(&module[0]).and_then(|names| names.get(name)) {
            return Some(vec![target.clone()]);
        }
        // Rust's implicit library crates participate in name resolution;
        // they do not grant construction authority.
        if self.modules.contains_key(&vec![name.to_string()])
            || matches!(name, "std" | "core" | "alloc")
        {
            return Some(vec![name.to_string()]);
        }
        None
    }
}

fn unique(mut paths: BTreeSet<ItemPath>) -> Result<ItemPath, ResolutionError> {
    match paths.len() {
        0 => Err(ResolutionError::Missing),
        1 => Ok(paths.pop_first().expect("one resolved path")),
        _ => Err(ResolutionError::Ambiguous(paths)),
    }
}

fn position(location: proc_macro2::LineColumn) -> Position {
    (location.line, location.column)
}

fn macro_key(file: &str, node: &syn::Macro) -> (String, Position, ItemPath, String) {
    // Expanded tokens can share the invocation's span. Their paths and
    // arguments distinguish nested calls from the invocation itself.
    (
        file.to_string(),
        position(node.path.span().start()),
        qualified_path(&node.path),
        node.tokens.to_string(),
    )
}

struct Collector<'a> {
    resolver: &'a mut NameResolver,
    file: &'a str,
    scope: usize,
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

    fn declare(&mut self, ident: &syn::Ident, namespace: Namespace) -> ItemPath {
        let mut path = self.resolver.scopes[self.scope].identity.clone();
        path.push(ident.to_string());
        self.resolver.scopes[self.scope]
            .names
            .entry(ident.to_string())
            .or_default()
            .push(Binding::Definition(path.clone(), namespace));
        self.resolver
            .definitions
            .insert((self.file.to_string(), position(ident.span().start())), path.clone());
        path
    }

    fn import(&mut self, tree: &syn::UseTree, prefix: &mut ItemPath) {
        match tree {
            syn::UseTree::Path(path) => {
                prefix.push(path.ident.to_string());
                self.import(&path.tree, prefix);
                prefix.pop();
            }
            syn::UseTree::Group(group) => {
                for tree in &group.items {
                    self.import(tree, prefix);
                }
            }
            syn::UseTree::Glob(_) => self.resolver.scopes[self.scope].globs.push(prefix.clone()),
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
                    .push(Binding::Import(path));
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
                    .push(Binding::Import(path));
            }
        }
    }
}

impl<'ast> Visit<'ast> for Collector<'_> {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        let (attrs, ident) = match node {
            syn::Item::Fn(item) => (&item.attrs, Some(&item.sig.ident)),
            syn::Item::Struct(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Enum(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Union(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Type(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Trait(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Const(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Static(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Mod(item) => (&item.attrs, None),
            syn::Item::Impl(item) => (&item.attrs, None),
            syn::Item::Use(item) => (&item.attrs, None),
            syn::Item::Macro(item) => (&item.attrs, None),
            _ => {
                visit::visit_item(self, node);
                return;
            }
        };
        if is_test_only(attrs) {
            return;
        }
        if let Some(ident) = ident {
            let namespace =
                if matches!(node, syn::Item::Fn(_) | syn::Item::Const(_) | syn::Item::Static(_)) {
                    Namespace::Value
                } else {
                    Namespace::Type
                };
            let path = self.declare(ident, namespace);
            if matches!(node, syn::Item::Struct(_)) {
                self.declare(ident, Namespace::Value);
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
        self.import(&node.tree, &mut prefix);
    }

    fn visit_item_extern_crate(&mut self, node: &'ast syn::ItemExternCrate) {
        if is_test_only(&node.attrs) {
            return;
        }
        let name = node.rename.as_ref().map_or(&node.ident, |(_, name)| name).to_string();
        let target = if node.ident == "self" {
            self.resolver.scopes[self.scope].module[0].clone()
        } else {
            node.ident.to_string()
        };
        self.resolver.scopes[self.scope]
            .names
            .entry(name)
            .or_default()
            .push(Binding::Definition(vec![target], Namespace::Type));
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        let scope = self.resolver.local_module(self.scope, &node.ident.to_string());
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
        if !is_test_only(&node.attrs) {
            self.scoped(node.span(), |collector| visit::visit_impl_item_fn(collector, node));
        }
    }

    fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
        if !is_test_only(&node.attrs) {
            self.scoped(node.span(), |collector| visit::visit_trait_item_fn(collector, node));
        }
    }

    fn visit_type_param(&mut self, node: &'ast syn::TypeParam) {
        self.declare(&node.ident, Namespace::Type);
    }

    fn visit_foreign_item_fn(&mut self, node: &'ast syn::ForeignItemFn) {
        if !is_test_only(&node.attrs) {
            self.declare(&node.sig.ident, Namespace::Value);
        }
    }

    fn visit_item_macro(&mut self, node: &'ast syn::ItemMacro) {
        if let Some(name) = &node.ident {
            if node.mac.path.is_ident("macro_rules") {
                self.resolver.scopes[self.scope].macros.insert(name.to_string(), node.mac.clone());
                return;
            }
        }
        if let Some(name) = node.mac.path.get_ident() {
            let mut scope = Some(self.scope);
            while let Some(current) = scope {
                if let Some(definition) =
                    self.resolver.scopes[current].macros.get(&name.to_string())
                {
                    if let Some(expanded) = crate::macro_expansion::expand(definition, &node.mac) {
                        self.resolver
                            .expanded
                            .insert(macro_key(self.file, &node.mac), expanded.clone());
                        self.visit_file(&expanded);
                        return;
                    }
                    break;
                }
                scope = self.resolver.scopes[current].parent;
            }
        }
        visit::visit_item_macro(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

pub(crate) fn qualified_path(path: &syn::Path) -> ItemPath {
    let mut names = path_names(path);
    if path.leading_colon.is_some() {
        names.insert(0, String::new());
    }
    names
}
