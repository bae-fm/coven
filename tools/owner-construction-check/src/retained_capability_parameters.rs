//! A method never takes a raw capability as a parameter (§20.2), such as a
//! store directory: it is fixed when the owner graph is built, and the owner
//! uses the one it retains. Only an owner's constructors and the composition
//! roots accept the policy's construction-only capabilities.

use std::collections::BTreeSet;

use syn::spanned::Spanned;

use crate::capability_construction::construction_only_types;
use crate::owner_construction::Constructor;
use crate::policy::Policy;
use crate::syntax::{is_test_only, is_test_source, type_name, type_names, RustFile};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct RetainedCapabilityParameterViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) owner: String,
    pub(crate) method: String,
    pub(crate) capability: String,
}

pub(crate) fn find_retained_capability_parameter_violations(
    files: &[RustFile],
    owners: &BTreeSet<String>,
    constructors: &BTreeSet<Constructor>,
    policy: &Policy,
) -> Vec<RetainedCapabilityParameterViolation> {
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
            constructors,
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
    constructors: &BTreeSet<Constructor>,
    policy: &Policy,
    capabilities: &BTreeSet<String>,
    violations: &mut BTreeSet<RetainedCapabilityParameterViolation>,
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
                    let callable = Constructor {
                        owner: owner.clone(),
                        method: method.sig.ident.to_string(),
                    };
                    if constructors.contains(&callable)
                        || policy.composition_roots.iter().any(
                            |(root_path, root_owner, root_method)| {
                                path == *root_path
                                    && owner == *root_owner
                                    && method.sig.ident == *root_method
                            },
                        )
                    {
                        continue;
                    }
                    for input in &method.sig.inputs {
                        let syn::FnArg::Typed(input) = input else {
                            continue;
                        };
                        let names = type_names(&input.ty);
                        for capability in capabilities {
                            if names.contains(capability) {
                                violations.insert(RetainedCapabilityParameterViolation {
                                    path: path.to_string(),
                                    line: input.span().start().line,
                                    owner: owner.clone(),
                                    method: method.sig.ident.to_string(),
                                    capability: capability.clone(),
                                });
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
                    find_in_items(
                        path,
                        items,
                        owners,
                        constructors,
                        policy,
                        capabilities,
                        violations,
                    );
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
#[path = "retained_capability_parameters_tests.rs"]
mod tests;
