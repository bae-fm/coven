//! Factory results are indexed by resolved crate/module/type paths.

use std::collections::{BTreeMap, BTreeSet};
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::finding::Finding;
use crate::macros::parse_macro_body;
use crate::name_resolution::{qualified_path, ItemPath, NameResolver, ResolutionError};
use crate::syntax::{is_test_only, is_test_source, type_names, RustFile};

pub(crate) struct Factories {
    pub(crate) results: BTreeMap<ItemPath, BTreeMap<Option<ItemPath>, BTreeSet<String>>>,
    /// These names only determine whether failed resolution needs a diagnostic.
    /// They never select a definition or contribute its results.
    pub(crate) free_names: BTreeSet<String>,
    pub(crate) associated_types: BTreeSet<String>,
}

impl Factories {
    pub(crate) fn products(
        &self,
        path: &[String],
        trait_path: Option<&ItemPath>,
        trait_in_scope: impl Fn(&ItemPath) -> bool,
    ) -> Result<Option<&BTreeSet<String>>, ResolutionError> {
        let Some(definitions) = self.results.get(path) else {
            return Ok(None);
        };
        if let Some(trait_path) = trait_path {
            return definitions
                .get(&Some(trait_path.clone()))
                .map(Some)
                .ok_or(ResolutionError::Missing);
        }
        if let Some(inherent) = definitions.get(&None) {
            return Ok(Some(inherent));
        }
        let candidates = definitions
            .iter()
            .filter_map(|(path, products)| {
                path.as_ref()
                    .filter(|path| trait_in_scope(path))
                    .map(|path| (path, products))
            })
            .collect::<Vec<_>>();
        match candidates.as_slice() {
            [] => Err(ResolutionError::Missing),
            [(_, products)] => Ok(Some(products)),
            _ => Err(ResolutionError::Ambiguous(
                candidates.iter().map(|(path, _)| (*path).clone()).collect(),
            )),
        }
    }

    pub(crate) fn collect(
        files: &[RustFile],
        constructed: &BTreeSet<String>,
        resolver: &NameResolver,
        findings: &mut BTreeSet<Finding>,
    ) -> Self {
        let mut factories = Self {
            results: BTreeMap::new(),
            free_names: BTreeSet::new(),
            associated_types: BTreeSet::new(),
        };
        let mut traits = BTreeMap::new();
        for file in files.iter().filter(|file| {
            !is_test_source(&file.relative_path) && !is_test_only(&file.syntax.attrs)
        }) {
            TraitCollector {
                path: &file.relative_path,
                resolver,
                traits: &mut traits,
            }
            .visit_file(&file.syntax);
        }
        let mut aliases = Vec::new();
        for file in files.iter().filter(|file| {
            !is_test_source(&file.relative_path) && !is_test_only(&file.syntax.attrs)
        }) {
            Collector {
                path: &file.relative_path,
                resolver,
                constructed,
                traits: &traits,
                factories: &mut factories,
                aliases: &mut aliases,
                findings,
            }
            .visit_file(&file.syntax);
        }
        loop {
            let mut inherited = BTreeMap::new();
            for (alias, target) in &aliases {
                for (path, results) in &factories.results {
                    if path.len() != target.len() + 1 || !path.starts_with(target) {
                        continue;
                    }
                    let mut key = alias.clone();
                    key.push(path.last().expect("method").clone());
                    if !factories.results.contains_key(&key) {
                        inherited.insert(
                            key,
                            results
                                .iter()
                                .map(|(trait_path, names)| {
                                    (
                                        trait_path.clone(),
                                        names
                                            .iter()
                                            .map(|name| {
                                                if Some(name) == target.last() {
                                                    alias.last().expect("alias").clone()
                                                } else {
                                                    name.clone()
                                                }
                                            })
                                            .collect(),
                                    )
                                })
                                .collect(),
                        );
                    }
                }
            }
            if inherited.is_empty() {
                break;
            }
            factories.results.extend(inherited);
        }
        factories
    }
}

struct Collector<'a> {
    path: &'a str,
    resolver: &'a NameResolver,
    constructed: &'a BTreeSet<String>,
    traits: &'a BTreeMap<ItemPath, Vec<(String, BTreeSet<String>)>>,
    factories: &'a mut Factories,
    aliases: &'a mut Vec<(ItemPath, ItemPath)>,
    findings: &'a mut BTreeSet<Finding>,
}

impl Collector<'_> {
    fn resolve(&mut self, path: &syn::Path) -> Option<ItemPath> {
        let names = qualified_path(path);
        match self.resolver.resolve_type(self.path, path.span(), &names) {
            Ok(path) => Some(path),
            Err(error) => {
                self.findings.insert(Finding::new(
                    self.path,
                    path.span().start().line,
                    format!(
                        "cannot resolve factory definition {}: {error}",
                        names.join("::")
                    ),
                    "name factory types and traits through an unambiguous module path or import",
                ));
                None
            }
        }
    }

    fn record(
        &mut self,
        mut factory: ItemPath,
        method: &str,
        names: &BTreeSet<String>,
        trait_path: Option<ItemPath>,
    ) {
        let owner = factory.last().expect("factory type");
        let mut results = names
            .intersection(self.constructed)
            .cloned()
            .collect::<BTreeSet<_>>();
        if names.contains("Self") && self.constructed.contains(owner) {
            results.insert(owner.clone());
        }
        if !results.is_empty() {
            self.factories.associated_types.insert(owner.clone());
        }
        factory.push(method.to_string());
        self.factories
            .results
            .entry(factory)
            .or_default()
            .entry(trait_path)
            .or_default()
            .extend(results);
    }
}

impl<'ast> Visit<'ast> for Collector<'_> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        if let syn::ReturnType::Type(_, output) = &node.sig.output {
            let results = type_names(output)
                .intersection(self.constructed)
                .cloned()
                .collect::<BTreeSet<_>>();
            if !results.is_empty() {
                self.factories.free_names.insert(node.sig.ident.to_string());
                self.factories
                    .results
                    .entry(self.resolver.definition(self.path, &node.sig.ident))
                    .or_default()
                    .entry(None)
                    .or_default()
                    .extend(results);
            }
        }
        visit::visit_item_fn(self, node);
    }

    fn visit_item_enum(&mut self, node: &'ast syn::ItemEnum) {
        if !is_test_only(&node.attrs) && self.constructed.contains(&node.ident.to_string()) {
            for variant in &node.variants {
                self.record(
                    self.resolver.definition(self.path, &node.ident),
                    &variant.ident.to_string(),
                    &BTreeSet::from([node.ident.to_string()]),
                    None,
                );
            }
        }
    }

    fn visit_item_type(&mut self, node: &'ast syn::ItemType) {
        if !is_test_only(&node.attrs) && self.constructed.contains(&node.ident.to_string()) {
            if let syn::Type::Path(target) = node.ty.as_ref() {
                if let Some(target) = self.resolve(&target.path) {
                    self.factories
                        .associated_types
                        .insert(node.ident.to_string());
                    self.aliases
                        .push((self.resolver.definition(self.path, &node.ident), target));
                }
            }
        }
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if is_test_only(&node.attrs) {
            return;
        }
        let syn::Type::Path(ty) = node.self_ty.as_ref() else {
            return;
        };
        let trait_path = match &node.trait_ {
            Some((path, _)) => match self.resolve(path) {
                Some(path) => Some(path),
                None => {
                    visit::visit_item_impl(self, node);
                    return;
                }
            },
            None => None,
        };
        let mut methods = trait_path
            .as_ref()
            .and_then(|path| self.traits.get(path))
            .cloned()
            .unwrap_or_default();
        for item in &node.items {
            if let syn::ImplItem::Fn(method) = item {
                if !is_test_only(&method.attrs) {
                    let names = match &method.sig.output {
                        syn::ReturnType::Type(_, output) => type_names(output),
                        syn::ReturnType::Default => BTreeSet::new(),
                    };
                    methods.retain(|(name, _)| method.sig.ident != name.as_str());
                    methods.push((method.sig.ident.to_string(), names));
                }
            }
        }
        let factory = self.resolve(&ty.path);
        if let Some(factory) = factory {
            for (method, names) in methods {
                self.record(factory.clone(), &method, &names, trait_path.clone());
            }
        }
        visit::visit_item_impl(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(expanded) = self.resolver.expanded_macro(self.path, node) {
            self.visit_file(expanded);
        } else if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

struct TraitCollector<'a> {
    path: &'a str,
    resolver: &'a NameResolver,
    traits: &'a mut BTreeMap<ItemPath, Vec<(String, BTreeSet<String>)>>,
}

impl<'ast> Visit<'ast> for TraitCollector<'_> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
        if is_test_only(&node.attrs) {
            return;
        }
        let methods = self
            .traits
            .entry(self.resolver.definition(self.path, &node.ident))
            .or_default();
        for item in &node.items {
            if let syn::TraitItem::Fn(method) = item {
                if !is_test_only(&method.attrs) {
                    let names = match &method.sig.output {
                        syn::ReturnType::Type(_, output) => type_names(output),
                        syn::ReturnType::Default => BTreeSet::new(),
                    };
                    methods.push((method.sig.ident.to_string(), names));
                }
            }
        }
        visit::visit_item_trait(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(expanded) = self.resolver.expanded_macro(self.path, node) {
            self.visit_file(expanded);
        } else if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}
