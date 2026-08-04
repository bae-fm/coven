//! Module dependency direction: the architecture's arrow diagram as a gate.
//!
//! ```text
//! Host API: Coven / CovenHandle
//!         │
//!         ▼
//! Rows · Blobs · Joining · Circles · Membership        (domain)
//!         │
//!         ▼
//! Replication ───────► Protocol model
//!         │            commits · identities · signed operations
//!         ├──────────► Database
//!         ├──────────► Storage
//!         └──────────► Keys / encryption
//! ```
//!
//! Every top-level module of the coven crate is assigned to a region, and a
//! production `crate::` reference from one region to another must point down
//! the diagram. Within-region references are unrestricted — this gate asserts
//! the direction between regions, not the structure inside one.
//!
//! References through root re-exports (`use crate::SomeItem`) resolve to the
//! item's source module via `lib.rs`, so a re-export cannot bypass the gate.

use std::collections::{BTreeMap, BTreeSet};

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::{is_test_only, is_test_source, RustFile};

/// Region rank orders the layers bottom-up. A reference from module A to
/// module B is allowed when B's region rank is strictly lower than A's, or the
/// regions are equal.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum Region {
    /// Injectable primitives and value modules with no coven dependencies of
    /// their own: clocks, ids, staged files, layout, configuration.
    Foundation,
    /// Key custody, encryption, and sealed-secret owners.
    Keys,
    /// The deterministic protocol model: signed values, parsing, validation.
    Protocol,
    /// The SQLite boundary.
    Database,
    /// Cloud providers, local file storage, and the OAuth flow they use.
    Storage,
    /// Replication: the sync loop, Store authority spine, verified history.
    Replication,
    /// Domain workflows over replication: blobs, joining, restore, Circles.
    Domain,
    /// The host-facing composition roots and their retained owners.
    Host,
}

/// Every top-level module must appear here; an unassigned module fails the
/// check so new modules are classified when they are introduced.
pub(crate) const MODULE_REGIONS: &[(&str, Region)] = &[
    ("atomic_file", Region::Foundation),
    ("changeset", Region::Foundation),
    ("clock", Region::Foundation),
    ("code_envelope", Region::Foundation),
    ("config", Region::Foundation),
    ("id_provider", Region::Foundation),
    ("local_file", Region::Foundation),
    ("object_hash", Region::Foundation),
    ("store_dir", Region::Foundation),
    ("write", Region::Foundation),
    ("custody", Region::Keys),
    ("encryption", Region::Keys),
    ("envelope", Region::Keys),
    ("identity_custody", Region::Keys),
    ("keyring_backend", Region::Keys),
    ("keys", Region::Keys),
    ("protocol", Region::Protocol),
    ("database", Region::Database),
    ("oauth", Region::Storage),
    ("storage", Region::Storage),
    ("sync", Region::Replication),
    ("blob", Region::Domain),
    ("joining", Region::Domain),
    ("restoration", Region::Domain),
    ("circles", Region::Host),
    ("coven", Region::Host),
    ("handle", Region::Host),
    ("read_handle", Region::Host),
    ("read_store_rows", Region::Host),
    ("store_blobs", Region::Host),
    ("store_circles", Region::Host),
    ("store_cloud_storage", Region::Host),
    ("store_joining", Region::Host),
    ("store_membership", Region::Host),
    ("store_recovery", Region::Host),
    ("store_rows", Region::Host),
    ("store_security", Region::Host),
    ("store_sync", Region::Host),
];

/// `write` is a value module over protocol shapes; `protocol` sits above it in
/// the foundation ordering only because `write` consumes protocol values.
/// Rather than fold that one edge into the region ranks, it is the single
/// declared same-direction exception.
const EDGE_EXCEPTIONS: &[(&str, &str)] = &[("write", "protocol")];

/// Database and Storage are siblings: replication composes both, but neither
/// may reach into the other.
fn allows(from: Region, to: Region) -> bool {
    if from == to {
        return true;
    }
    if (from, to) == (Region::Database, Region::Storage)
        || (from, to) == (Region::Storage, Region::Database)
    {
        return false;
    }
    to < from
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct ModuleDependencyViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) from_region: Region,
    pub(crate) to_region: Region,
}

pub(crate) fn find_module_dependency_violations(
    files: &[RustFile],
) -> Result<Vec<ModuleDependencyViolation>, String> {
    let regions: BTreeMap<&str, Region> = MODULE_REGIONS.iter().copied().collect();
    let known_modules = top_level_modules(files);
    if let Some(unassigned) = known_modules
        .iter()
        .find(|module| !regions.contains_key(module.as_str()))
    {
        return Err(format!(
            "top-level module {unassigned} has no region assignment in MODULE_REGIONS"
        ));
    }
    let reexports = root_reexports(files, &known_modules);
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let Some(from) = file_module(&file.relative_path) else {
            continue;
        };
        if !known_modules.contains(&from) {
            continue;
        }
        let mut visitor = ModuleDependencyVisitor {
            path: &file.relative_path,
            from: &from,
            regions: &regions,
            known_modules: &known_modules,
            reexports: &reexports,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    Ok(violations.into_iter().collect())
}

/// The referencing file's top-level module: the first path component under
/// `crates/coven/src/`, or the file stem for root-level files. `lib.rs` is the
/// crate root and is exempt — it is the composition surface that names every
/// module.
fn file_module(path: &str) -> Option<String> {
    let relative = path.strip_prefix("crates/coven/src/")?;
    let top = relative.split('/').next()?;
    let module = top.strip_suffix(".rs").unwrap_or(top);
    if module == "lib" {
        return None;
    }
    Some(module.to_string())
}

fn top_level_modules(files: &[RustFile]) -> BTreeSet<String> {
    files
        .iter()
        .filter(|file| !is_test_source(&file.relative_path))
        .filter_map(|file| file_module(&file.relative_path))
        .collect()
}

/// Item name → source module for every root re-export in `lib.rs`
/// (`pub use module::Item`, including groups and renames).
fn root_reexports(
    files: &[RustFile],
    known_modules: &BTreeSet<String>,
) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for file in files {
        if file.relative_path != "crates/coven/src/lib.rs" {
            continue;
        }
        for item in &file.syntax.items {
            let syn::Item::Use(item) = item else {
                continue;
            };
            let mut paths = Vec::new();
            crate::capability_boundaries::flatten_use_tree(&item.tree, &mut Vec::new(), &mut paths);
            for segments in paths {
                let segments: Vec<&String> = segments
                    .iter()
                    .filter(|segment| *segment != "crate" && *segment != "self")
                    .collect();
                let (Some(first), Some(last)) = (segments.first(), segments.last()) else {
                    continue;
                };
                if known_modules.contains(first.as_str()) && first != last {
                    map.insert((*last).clone(), (*first).clone());
                }
            }
        }
    }
    map
}

struct ModuleDependencyVisitor<'a> {
    path: &'a str,
    from: &'a str,
    regions: &'a BTreeMap<&'static str, Region>,
    known_modules: &'a BTreeSet<String>,
    reexports: &'a BTreeMap<String, String>,
    violations: &'a mut BTreeSet<ModuleDependencyViolation>,
}

impl ModuleDependencyVisitor<'_> {
    /// `segments` is a `crate::`-rooted path with the `crate` segment removed.
    fn check_reference(&mut self, segments: &[String], line: usize) {
        let Some(first) = segments.first() else {
            return;
        };
        let to = if self.known_modules.contains(first) {
            first.clone()
        } else if let Some(source) = self.reexports.get(first) {
            source.clone()
        } else {
            return;
        };
        if to == self.from {
            return;
        }
        if EDGE_EXCEPTIONS
            .iter()
            .any(|(from, allowed)| *from == self.from && *allowed == to)
        {
            return;
        }
        let from_region = self.regions[self.from];
        let to_region = self.regions[to.as_str()];
        if allows(from_region, to_region) {
            return;
        }
        self.violations.insert(ModuleDependencyViolation {
            path: self.path.to_string(),
            line,
            from: self.from.to_string(),
            to,
            from_region,
            to_region,
        });
    }
}

impl<'ast> Visit<'ast> for ModuleDependencyVisitor<'_> {
    fn visit_attribute(&mut self, node: &'ast syn::Attribute) {
        if node.path().is_ident("doc") {
            return;
        }
        visit::visit_attribute(self, node);
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_item_mod(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_item_impl(self, node);
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_item_fn(self, node);
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_impl_item_fn(self, node);
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        let mut paths = Vec::new();
        crate::capability_boundaries::flatten_use_tree(&node.tree, &mut Vec::new(), &mut paths);
        for segments in paths {
            if segments.first().is_some_and(|segment| segment == "crate") {
                self.check_reference(&segments[1..], node.span().start().line);
            }
        }
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        let segments: Vec<String> = node
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect();
        if segments.first().is_some_and(|segment| segment == "crate") {
            self.check_reference(&segments[1..], node.span().start().line);
        }
        visit::visit_path(self, node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, source: &str) -> RustFile {
        RustFile {
            relative_path: path.to_string(),
            syntax: syn::parse_file(source).expect("parse fixture"),
        }
    }

    fn lib() -> RustFile {
        file(
            "crates/coven/src/lib.rs",
            "pub use database::StoreDatabase;",
        )
    }

    #[test]
    fn an_upward_reference_is_rejected() {
        let files = vec![
            lib(),
            file("crates/coven/src/database/store.rs", ""),
            file(
                "crates/coven/src/protocol/commit.rs",
                "use crate::sync::VerifiedThing;",
            ),
            file("crates/coven/src/sync/mod.rs", ""),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].from, "protocol");
        assert_eq!(violations[0].to, "sync");
    }

    #[test]
    fn a_downward_reference_is_allowed() {
        let files = vec![
            lib(),
            file("crates/coven/src/database/store.rs", ""),
            file(
                "crates/coven/src/sync/mod.rs",
                "use crate::database::StoreDatabase; use crate::protocol::Commit;",
            ),
            file("crates/coven/src/protocol/mod.rs", ""),
        ];
        assert!(find_module_dependency_violations(&files)
            .expect("check runs")
            .is_empty());
    }

    #[test]
    fn database_and_storage_are_mutually_closed_siblings() {
        let files = vec![
            lib(),
            file(
                "crates/coven/src/database/store.rs",
                "use crate::storage::ExactObjectRef;",
            ),
            file(
                "crates/coven/src/storage/remote.rs",
                "use crate::database::StoreDatabase;",
            ),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 2);
    }

    #[test]
    fn a_root_reexport_cannot_bypass_the_gate() {
        let files = vec![
            file(
                "crates/coven/src/lib.rs",
                "pub use sync::DeviceJoinJournalDatabase;",
            ),
            file("crates/coven/src/sync/mod.rs", ""),
            file(
                "crates/coven/src/protocol/mod.rs",
                "fn open() { let _ = crate::DeviceJoinJournalDatabase::open(\"p\"); }",
            ),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].to, "sync");
    }

    #[test]
    fn an_unassigned_module_fails_the_check() {
        let files = vec![file("crates/coven/src/brand_new_module.rs", "")];
        assert!(find_module_dependency_violations(&files).is_err());
    }

    #[test]
    fn test_sources_and_cfg_test_items_are_exempt() {
        let files = vec![
            lib(),
            file("crates/coven/src/sync/mod.rs", ""),
            file(
                "crates/coven/src/protocol/commit_tests.rs",
                "use crate::sync::VerifiedThing;",
            ),
            file(
                "crates/coven/src/protocol/mod.rs",
                r#"
                #[cfg(test)]
                mod tests { use crate::sync::VerifiedThing; }
                "#,
            ),
        ];
        assert!(find_module_dependency_violations(&files)
            .expect("check runs")
            .is_empty());
    }
}
