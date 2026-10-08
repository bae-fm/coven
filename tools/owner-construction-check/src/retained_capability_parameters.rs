//! A method never takes a raw capability as a parameter (§20.2), such as a
//! store directory: it is fixed when the owner graph is built, and the owner
//! uses the one it retains. Only an owner's constructors and the composition
//! roots accept the policy's construction-only capabilities.

use std::collections::BTreeSet;

use syn::spanned::Spanned;

use crate::capability_construction::construction_only_types;
use crate::finding::Finding;
use crate::policy::Policy;
use crate::syntax::{
    is_test_only, is_test_source, output_contains_owner, supplied_type_names, type_name, RustFile,
};

pub(crate) fn find_retained_capability_parameter_violations(
    files: &[RustFile],
    owners: &BTreeSet<String>,
    policy: &Policy,
) -> Vec<Finding> {
    let capabilities = construction_only_types(files, policy);
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        find_in_items(
            &file.relative_path,
            &file.syntax.items,
            owners,
            policy,
            &capabilities,
            &mut violations,
        );
    }
    violations.into_iter().collect()
}

fn find_in_items(
    path: &str,
    items: &[syn::Item],
    owners: &BTreeSet<String>,
    policy: &Policy,
    capabilities: &BTreeSet<String>,
    violations: &mut BTreeSet<Finding>,
) {
    for item in items {
        match item {
            syn::Item::Impl(item) => {
                if is_test_only(&item.attrs) {
                    continue;
                }
                let Some(owner) = type_name(&item.self_ty) else {
                    continue;
                };
                if !owners.contains(&owner) {
                    continue;
                }
                for impl_item in &item.items {
                    let syn::ImplItem::Fn(method) = impl_item else {
                        continue;
                    };
                    if is_test_only(&method.attrs) {
                        continue;
                    }
                    if output_contains_owner(&method.sig.output, &owner)
                        || policy.is_composition_root(path, &owner, &method.sig.ident.to_string())
                    {
                        continue;
                    }
                    for input in &method.sig.inputs {
                        let syn::FnArg::Typed(input) = input else {
                            continue;
                        };
                        let names = supplied_type_names(&input.ty);
                        for capability in capabilities {
                            if names.contains(capability) {
                                violations.insert(Finding::new(path, input.span().start().line,
                                    format!("{owner}::{} accepts construction-only capability {capability} at runtime", method.sig.ident),
                                    "a method never takes a raw capability; it uses the one its owner was built with"));
                            }
                        }
                    }
                }
            }
            syn::Item::Mod(item) => {
                if is_test_only(&item.attrs) {
                    continue;
                }
                if let Some((_, items)) = &item.content {
                    find_in_items(path, items, owners, policy, capabilities, violations);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
#[path = "retained_capability_parameters_tests.rs"]
mod tests;
