//! The guard tests of §21.4: every file, crate, type and composition root the
//! policy file names must exist, so the policy can't go stale. A row keyed on
//! a path that moved does not fail a rule — it stops matching, and the rule
//! silently covers nothing — so each one is resolved against the workspace
//! here instead.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use syn::visit::{self, Visit};

use super::POLICY;
use crate::capability_construction::{
    construction_only_types, find_capability_construction_violations,
};
use crate::policy::Policy;
use crate::sources::load;
use crate::syntax::{
    collect_declared_types, is_test_only, is_test_source, type_name, type_names, RustFile,
};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the checker lives two directories under the workspace root")
        .to_path_buf()
}

/// Capability homes name the paths §21.2 assigns ahead of the code. A home
/// inside a crate that has not landed is skipped; one inside a crate that has
/// must exist.
fn stranded_homes(root: &Path, policy: &Policy) -> Vec<String> {
    policy
        .capabilities
        .all()
        .into_iter()
        .flat_map(|capability| {
            capability
                .homes
                .iter()
                .map(move |home| (capability.name, *home))
        })
        .filter(|(_, home)| {
            let mut segments = home.split('/');
            match (segments.next(), segments.next()) {
                (Some("crates"), Some(name)) => {
                    root.join("crates").join(name).join("Cargo.toml").is_file()
                }
                _ => true,
            }
        })
        .filter(|(_, home)| {
            let resolved = root.join(home);
            if home.ends_with('/') {
                !resolved.is_dir()
            } else {
                !resolved.is_file()
            }
        })
        .map(|(capability, home)| format!("{capability}: {home}"))
        .collect()
}

#[test]
fn every_capability_home_in_a_landed_crate_exists() {
    let stranded = stranded_homes(&workspace_root(), &POLICY);
    assert!(
        stranded.is_empty(),
        "capability homes no longer resolve, so the capabilities keyed on them cover nothing:\n{}",
        stranded.join("\n")
    );
}

#[test]
fn a_home_is_skipped_until_its_crate_lands_and_checked_after() {
    let root = std::env::temp_dir().join(format!(
        "owner-construction-check-homes-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let clock_crate = root.join("crates/coven-foundation");
    std::fs::create_dir_all(clock_crate.join("src")).expect("create fixture crate");
    std::fs::write(clock_crate.join("Cargo.toml"), "").expect("write fixture manifest");

    let stranded = stranded_homes(&root, &POLICY);
    std::fs::remove_dir_all(&root).expect("remove fixture workspace");

    let foundation_homes = POLICY
        .capabilities
        .all()
        .into_iter()
        .flat_map(|capability| {
            capability
                .homes
                .iter()
                .filter(|home| home.starts_with("crates/coven-foundation/"))
                .map(move |home| format!("{}: {home}", capability.name))
        })
        .collect::<Vec<_>>();
    assert!(!foundation_homes.is_empty());
    assert_eq!(stranded, foundation_homes);
}

fn declared_methods(
    files: &[RustFile],
    include_tests: bool,
) -> BTreeMap<(String, String, String), syn::Signature> {
    struct MethodCollector<'a> {
        path: &'a str,
        methods: &'a mut BTreeMap<(String, String, String), syn::Signature>,
        include_tests: bool,
    }

    impl Visit<'_> for MethodCollector<'_> {
        fn visit_item_fn(&mut self, node: &syn::ItemFn) {
            if self.include_tests || !is_test_only(&node.attrs) {
                self.methods.insert(
                    (
                        self.path.to_string(),
                        "<free>".into(),
                        node.sig.ident.to_string(),
                    ),
                    node.sig.clone(),
                );
                visit::visit_item_fn(self, node);
            }
        }
        fn visit_item_mod(&mut self, node: &syn::ItemMod) {
            if self.include_tests || !is_test_only(&node.attrs) {
                visit::visit_item_mod(self, node);
            }
        }

        fn visit_item_impl(&mut self, node: &syn::ItemImpl) {
            if !self.include_tests && is_test_only(&node.attrs) {
                return;
            }
            let Some(owner) = type_name(&node.self_ty) else {
                return;
            };
            for item in &node.items {
                if let syn::ImplItem::Fn(method) = item {
                    if self.include_tests || !is_test_only(&method.attrs) {
                        self.methods.insert(
                            (
                                self.path.to_string(),
                                owner.clone(),
                                method.sig.ident.to_string(),
                            ),
                            method.sig.clone(),
                        );
                    }
                }
            }
            visit::visit_item_impl(self, node);
        }
    }

    let mut methods = BTreeMap::new();
    for file in files
        .iter()
        .filter(|file| include_tests || !is_test_source(&file.relative_path))
    {
        MethodCollector {
            path: &file.relative_path,
            methods: &mut methods,
            include_tests,
        }
        .visit_file(&file.syntax);
    }
    methods
}

#[test]
fn every_composition_root_names_an_existing_method() {
    let workspace = load(&workspace_root()).expect("read the workspace");
    let methods = declared_methods(&workspace.files, true);
    let missing = POLICY
        .composition_roots
        .iter()
        .filter(|(path, owner, method)| {
            !methods.contains_key(&(
                (*path).to_string(),
                (*owner).to_string(),
                (*method).to_string(),
            ))
        })
        .map(|(path, owner, method)| format!("{path}: {owner}::{method}"))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "composition roots name methods that do not exist:\n{}",
        missing.join("\n")
    );
}

#[test]
fn composition_root_guards_can_name_test_fixture_methods() {
    let path = "crates/coven/src/builder_tests.rs";
    let files = [RustFile::fixture(path, "impl Fixture { fn open() {} }")];
    assert!(declared_methods(&files, true).contains_key(&(
        path.into(),
        "Fixture".into(),
        "open".into()
    )));
    assert!(declared_methods(&files, false).is_empty());
}

#[test]
fn composition_root_guards_resolve_free_functions() {
    let path = "crates/coven/src/bootstrap.rs";
    let files = [RustFile::fixture(
        path,
        "fn restore() {} #[cfg(test)] fn fixture() {}",
    )];
    let methods = declared_methods(&files, false);
    assert!(methods.contains_key(&(path.into(), "<free>".into(), "restore".into())));
    assert!(!methods.contains_key(&(path.into(), "<free>".into(), "fixture".into())));
}

fn invalid_capability_factories(files: &[RustFile], policy: &Policy) -> Vec<String> {
    let methods = declared_methods(files, false);
    let capabilities = construction_only_types(files, policy);
    policy
        .capability_factories
        .iter()
        .filter(|(path, owner, method, product)| {
            let signature = methods.get(&(path.to_string(), owner.to_string(), method.to_string()));
            !policy.capability_types.contains(owner)
                || !capabilities.contains(*product)
                || !signature.is_some_and(|signature| {
                    signature.receiver().is_some()
                        && match &signature.output {
                            syn::ReturnType::Type(_, output) => {
                                type_names(output).contains(*product)
                            }
                            syn::ReturnType::Default => false,
                        }
                })
        })
        .map(|(path, owner, method, product)| format!("{path}: {owner}::{method} -> {product}"))
        .collect()
}

#[test]
fn every_capability_factory_names_a_receiver_and_its_capability_product() {
    let workspace = load(&workspace_root()).expect("read the workspace");
    let invalid = invalid_capability_factories(&workspace.files, &POLICY);
    assert!(
        invalid.is_empty(),
        "invalid capability factories: {invalid:?}"
    );
}

#[test]
fn capability_factory_guards_reject_stale_or_unscoped_authority() {
    const FACTORIES: Policy = Policy {
        capability_types: &["StoreDir"],
        construction_only_capability_types: &["AtomicFile"],
        capability_factories: &[(
            "crates/coven-foundation/src/directory.rs",
            "StoreDir",
            "file",
            "AtomicFile",
        )],
        ..Policy::EMPTY
    };
    for (path, declaration, valid) in [
        (
            "crates/coven-foundation/src/directory.rs",
            "impl StoreDir { fn file(&self) -> AtomicFile {} }",
            true,
        ),
        (
            "crates/coven-foundation/src/renamed.rs",
            "impl StoreDir { fn file(&self) -> AtomicFile {} }",
            false,
        ),
        (
            "crates/coven-foundation/src/directory.rs",
            "impl Other { fn file(&self) -> AtomicFile {} }",
            false,
        ),
        (
            "crates/coven-foundation/src/directory.rs",
            "impl StoreDir { fn renamed(&self) -> AtomicFile {} }",
            false,
        ),
        (
            "crates/coven-foundation/src/directory.rs",
            "impl StoreDir { fn file() -> AtomicFile {} }",
            false,
        ),
        (
            "crates/coven-foundation/src/directory.rs",
            "impl StoreDir { fn file(&self) -> Other {} }",
            false,
        ),
        (
            "crates/coven-foundation/src/directory.rs",
            "impl StoreDir { #[cfg(test)] fn file(&self) -> AtomicFile {} }",
            false,
        ),
    ] {
        let invalid =
            invalid_capability_factories(&[RustFile::fixture(path, declaration)], &FACTORIES);
        assert_eq!(invalid.is_empty(), valid, "{path}: {declaration}");
    }
}

#[test]
fn native_keychain_acquisition_is_checked_at_its_real_factory_use_site() {
    let mut roots = POLICY.composition_roots.to_vec();
    roots.push(("crates/coven/src/builder.rs", "Builder", "open"));
    let rooted = Policy {
        composition_roots: Box::leak(roots.into_boxed_slice()),
        ..POLICY
    };
    let mut files = load(&workspace_root())
        .expect("read the workspace")
        .files
        .into_iter()
        .filter(RustFile::is_crate_source)
        .collect::<Vec<_>>();
    files.push(RustFile::fixture("crates/coven/src/builder.rs", "impl Builder {\n    fn open() { let _ = Keychain::registered(); }\n    fn run(&self) { let _ = Keychain::registered(); }\n}"));
    let violations = find_capability_construction_violations(&files, &rooted);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert_eq!(violations[0].path, "crates/coven/src/builder.rs");
    assert_eq!(violations[0].line, 3);
    assert_eq!(violations[0].capability, "Keychain");
}

#[test]
fn every_type_the_policy_names_is_declared() {
    let workspace = load(&workspace_root()).expect("read the workspace");
    let crate_files = workspace
        .files
        .into_iter()
        .filter(|file| file.is_crate_source())
        .collect::<Vec<_>>();
    let declared = collect_declared_types(&crate_files);
    let missing = POLICY
        .named_types()
        .into_iter()
        .filter(|(_, name)| !declared.contains_key(*name))
        .map(|(row, name)| format!("{row}: {name}"))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "the policy names types no crate declares:\n{}",
        missing.join("\n")
    );
}

fn missing_capability_traits(files: &[RustFile], policy: &Policy) -> Vec<String> {
    struct Traits(BTreeSet<String>);

    impl<'ast> Visit<'ast> for Traits {
        fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
            self.0.insert(node.ident.to_string());
            visit::visit_item_trait(self, node);
        }
    }

    let mut traits = Traits(BTreeSet::new());
    for file in files {
        traits.visit_file(&file.syntax);
    }
    policy
        .capability_traits
        .iter()
        .filter(|name| !traits.0.contains(**name))
        .map(|name| (*name).to_string())
        .collect()
}

#[test]
fn every_capability_trait_the_policy_names_is_a_declared_trait() {
    let workspace = load(&workspace_root()).expect("read the workspace");
    let files = workspace
        .files
        .into_iter()
        .filter(RustFile::is_crate_source)
        .collect::<Vec<_>>();
    let missing = missing_capability_traits(&files, &POLICY);
    assert!(
        missing.is_empty(),
        "capability_traits names interfaces no crate declares: {missing:?}"
    );
}

#[test]
fn capability_trait_guards_reject_other_types_and_missing_names() {
    const POLICY: Policy = Policy {
        capability_traits: &["Clock", "IdSource", "Missing"],
        ..Policy::EMPTY
    };
    let files = [RustFile::fixture(
        "crates/coven-foundation/src/lib.rs",
        r#"
        struct Clock;
        type IdSource = Clock;
    "#,
    )];
    assert_eq!(
        missing_capability_traits(&files, &POLICY),
        ["Clock", "IdSource", "Missing"]
    );
    let files = [RustFile::fixture(
        "crates/coven-foundation/src/lib.rs",
        r#"
        mod clock { trait Clock {} }
        #[cfg(feature = "test-utils")] mod ids { trait IdSource {} }
    "#,
    )];
    assert_eq!(missing_capability_traits(&files, &POLICY), ["Missing"]);
}

#[test]
fn every_crate_the_order_names_is_in_the_workspace() {
    let workspace = load(&workspace_root()).expect("read the workspace");
    let packages = workspace
        .manifests
        .iter()
        .filter(|manifest| manifest.relative_path.starts_with("crates/"))
        .filter_map(|manifest| manifest.table.get("package")?.get("name")?.as_str())
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let missing = POLICY
        .crate_order
        .iter()
        .chain(
            POLICY
                .separated_crates
                .iter()
                .flat_map(|(first, second)| [first, second]),
        )
        .filter(|name| !packages.contains(**name))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "crate_order and separated_crates name crates not in the workspace: {missing:?}"
    );
    for (first, second) in POLICY.separated_crates {
        assert!(POLICY.crate_order.contains(first) && POLICY.crate_order.contains(second));
    }
}

#[test]
fn the_database_schema_file_declares_its_table_macros() {
    let Some((schema_file, table_macros)) = POLICY.database_schema else {
        return;
    };
    let workspace = load(&workspace_root()).expect("read the workspace");
    let schema = workspace
        .files
        .iter()
        .find(|file| file.relative_path == schema_file)
        .unwrap_or_else(|| panic!("database schema file {schema_file} does not exist"));
    let declared = schema
        .syntax
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Macro(item) => item.ident.as_ref().map(ToString::to_string),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for table_macro in table_macros {
        assert!(
            declared.contains(*table_macro),
            "{schema_file} declares no macro_rules! {table_macro}"
        );
    }
}
