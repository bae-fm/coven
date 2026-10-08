//! Return types identify free and associated factories, including trait defaults.

use crate::syntax::{is_test_only, is_test_source, type_name, type_names, RustFile};
use std::collections::{BTreeMap, BTreeSet};
use syn::visit::{self, Visit};

pub(crate) fn collect_free_constructors(
    files: &[RustFile],
    owners: &BTreeSet<String>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut constructors = BTreeMap::new();
    for file in files
        .iter()
        .filter(|file| !is_test_source(&file.relative_path))
    {
        let mut collector = FreeConstructorCollector {
            owners,
            constructors: &mut constructors,
        };
        collector.visit_file(&file.syntax);
    }
    constructors
}

/// `(type, associated function)` → the owners it returns.
pub(crate) fn collect_associated_factories(
    files: &[RustFile],
    owners: &BTreeSet<String>,
) -> BTreeMap<(String, String), BTreeSet<String>> {
    let mut trait_outputs = BTreeMap::new();
    for file in files
        .iter()
        .filter(|file| !is_test_source(&file.relative_path))
    {
        TraitFactoryCollector {
            outputs: &mut trait_outputs,
        }
        .visit_file(&file.syntax);
    }
    let mut factories = BTreeMap::new();
    let mut aliases = BTreeMap::new();
    for file in files
        .iter()
        .filter(|file| !is_test_source(&file.relative_path))
    {
        let mut collector = AssociatedFactoryCollector {
            owners,
            trait_outputs: &trait_outputs,
            factories: &mut factories,
            aliases: &mut aliases,
        };
        collector.visit_file(&file.syntax);
    }
    loop {
        let mut inherited = BTreeMap::new();
        for (alias, target) in &aliases {
            for ((owner, method), results) in &factories {
                let key = (alias.clone(), method.clone());
                if owner == target && !factories.contains_key(&key) {
                    inherited.insert(
                        key,
                        results
                            .iter()
                            .map(|result| {
                                if result == target {
                                    alias.clone()
                                } else {
                                    result.clone()
                                }
                            })
                            .collect(),
                    );
                }
            }
        }
        if inherited.is_empty() {
            break;
        }
        factories.extend(inherited);
    }
    factories
}

struct AssociatedFactoryCollector<'a> {
    owners: &'a BTreeSet<String>,
    trait_outputs: &'a BTreeMap<String, Vec<(String, BTreeSet<String>)>>,
    factories: &'a mut BTreeMap<(String, String), BTreeSet<String>>,
    aliases: &'a mut BTreeMap<String, String>,
}

impl Visit<'_> for AssociatedFactoryCollector<'_> {
    fn visit_item_enum(&mut self, node: &syn::ItemEnum) {
        if !is_test_only(&node.attrs) && self.owners.contains(&node.ident.to_string()) {
            for variant in &node.variants {
                self.record(
                    &node.ident.to_string(),
                    &variant.ident.to_string(),
                    &BTreeSet::from([node.ident.to_string()]),
                );
            }
        }
    }

    fn visit_item_type(&mut self, node: &syn::ItemType) {
        if !is_test_only(&node.attrs) && self.owners.contains(&node.ident.to_string()) {
            if let Some(target) = type_name(&node.ty) {
                self.aliases.insert(node.ident.to_string(), target);
            }
        }
    }

    fn visit_item_mod(&mut self, node: &syn::ItemMod) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_impl(&mut self, node: &syn::ItemImpl) {
        if is_test_only(&node.attrs) {
            return;
        }
        let Some(factory) = type_name(&node.self_ty) else {
            return;
        };
        if let Some((trait_path, _)) = &node.trait_ {
            if let Some(methods) = trait_path
                .segments
                .last()
                .and_then(|name| self.trait_outputs.get(&name.ident.to_string()))
            {
                for (method, names) in methods {
                    self.record(&factory, method, names);
                }
            }
        }
        for item in &node.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            if is_test_only(&method.attrs) {
                continue;
            }
            let syn::ReturnType::Type(_, output) = &method.sig.output else {
                continue;
            };
            let names = type_names(output);
            self.record(&factory, &method.sig.ident.to_string(), &names);
        }
        visit::visit_item_impl(self, node);
    }
}

impl AssociatedFactoryCollector<'_> {
    fn record(&mut self, factory: &str, method: &str, names: &BTreeSet<String>) {
        let mut returned_owners = names
            .intersection(self.owners)
            .cloned()
            .collect::<BTreeSet<_>>();
        if names.contains("Self") && self.owners.contains(factory) {
            returned_owners.insert(factory.to_string());
        }
        if !returned_owners.is_empty() {
            self.factories
                .entry((factory.to_string(), method.to_string()))
                .or_default()
                .extend(returned_owners);
        }
    }
}

struct TraitFactoryCollector<'a> {
    outputs: &'a mut BTreeMap<String, Vec<(String, BTreeSet<String>)>>,
}

impl Visit<'_> for TraitFactoryCollector<'_> {
    fn visit_item_mod(&mut self, node: &syn::ItemMod) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_trait(&mut self, node: &syn::ItemTrait) {
        if is_test_only(&node.attrs) {
            return;
        }
        let methods = self.outputs.entry(node.ident.to_string()).or_default();
        for item in &node.items {
            if let syn::TraitItem::Fn(method) = item {
                if is_test_only(&method.attrs) {
                    continue;
                }
                if let syn::ReturnType::Type(_, output) = &method.sig.output {
                    methods.push((method.sig.ident.to_string(), type_names(output)));
                }
            }
        }
        visit::visit_item_trait(self, node);
    }
}

struct FreeConstructorCollector<'a> {
    owners: &'a BTreeSet<String>,
    constructors: &'a mut BTreeMap<String, BTreeSet<String>>,
}

impl Visit<'_> for FreeConstructorCollector<'_> {
    fn visit_item_mod(&mut self, node: &syn::ItemMod) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_fn(&mut self, node: &syn::ItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        let syn::ReturnType::Type(_, output) = &node.sig.output else {
            return;
        };
        let returned_owners = type_names(output)
            .intersection(self.owners)
            .cloned()
            .collect::<BTreeSet<_>>();
        if !returned_owners.is_empty() {
            self.constructors
                .entry(node.sig.ident.to_string())
                .or_default()
                .extend(returned_owners);
        }
    }
}
