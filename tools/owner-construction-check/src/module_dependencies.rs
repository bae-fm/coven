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
//! `crate::` reference from one region to another must point down the diagram.
//! Within-region references are unrestricted — this gate asserts the direction
//! between regions, not the structure inside one.
//!
//! Tests are held to the same direction as the code they exercise. A test that
//! reaches up a region is an integration test in the wrong module, or a fixture
//! that belongs to the layer whose types it builds; either way the regions
//! become crates that cannot depend on each other in that direction, test
//! profile included.
//!
//! References through root re-exports (`use crate::SomeItem`) resolve to the
//! item's source module via `lib.rs`, so a re-export cannot bypass the gate.

use std::collections::{BTreeMap, BTreeSet};

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::{is_test_source, RustFile};

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
    /// Replication: the sync loop, Store authority spine, verified history,
    /// and the blob locality/tombstone machinery its cycles execute.
    Replication,
    /// Domain workflows over replication: joining, restore.
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
    ("join_code", Region::Storage),
    ("restore_code", Region::Storage),
    ("oauth", Region::Storage),
    ("storage", Region::Storage),
    ("blob", Region::Replication),
    ("blocking", Region::Foundation),
    ("sync", Region::Replication),
    ("joining", Region::Domain),
    ("restoration", Region::Domain),
    ("circles", Region::Host),
    ("coven", Region::Host),
    ("handle", Region::Host),
    ("read_handle", Region::Host),
    ("store_blobs", Region::Host),
    ("store_circles", Region::Host),
    ("store_cloud_storage", Region::Host),
    ("store_foundation", Region::Host),
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
        let Some(from) = file_module(&file.relative_path) else {
            continue;
        };
        let from = if known_modules.contains(&from) {
            from
        } else if let Some(subject) = from
            .strip_suffix("_tests")
            .filter(|subject| known_modules.contains(*subject))
        {
            subject.to_string()
        } else {
            continue;
        };
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

/// A `<module>_tests` file or directory holds tests for `<module>` rather than
/// declaring a module of its own, so it never becomes a region of its own.
fn is_test_container(module: &str) -> bool {
    module.ends_with("_tests")
}

fn top_level_modules(files: &[RustFile]) -> BTreeSet<String> {
    files
        .iter()
        .filter(|file| !is_test_source(&file.relative_path))
        .filter_map(|file| file_module(&file.relative_path))
        .filter(|module| !is_test_container(module))
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

    /// Every `crate :: ident :: ident …` run in a macro's tokens, checked as a
    /// reference. Nested delimiters carry paths of their own, so groups are
    /// walked too.
    fn check_token_stream(&mut self, tokens: proc_macro2::TokenStream) {
        let trees: Vec<proc_macro2::TokenTree> = tokens.into_iter().collect();
        let mut index = 0;
        while index < trees.len() {
            if let proc_macro2::TokenTree::Group(group) = &trees[index] {
                self.check_token_stream(group.stream());
                index += 1;
                continue;
            }
            let proc_macro2::TokenTree::Ident(ident) = &trees[index] else {
                index += 1;
                continue;
            };
            if ident != "crate" {
                index += 1;
                continue;
            }
            let line = ident.span().start().line;
            let mut segments = Vec::new();
            let mut cursor = index + 1;
            while let Some(ident) = path_separator_then_ident(&trees, cursor) {
                segments.push(ident.to_string());
                cursor += 3;
            }
            if !segments.is_empty() {
                self.check_reference(&segments, line);
            }
            index = cursor.max(index + 1);
        }
    }
}

/// The identifier at `index` when the tokens there are `:: ident`.
fn path_separator_then_ident(
    trees: &[proc_macro2::TokenTree],
    index: usize,
) -> Option<&proc_macro2::Ident> {
    let (proc_macro2::TokenTree::Punct(first), proc_macro2::TokenTree::Punct(second)) =
        (trees.get(index)?, trees.get(index + 1)?)
    else {
        return None;
    };
    if first.as_char() != ':' || second.as_char() != ':' {
        return None;
    }
    if first.spacing() != proc_macro2::Spacing::Joint {
        return None;
    }
    match trees.get(index + 2)? {
        proc_macro2::TokenTree::Ident(ident) => Some(ident),
        _ => None,
    }
}

impl<'ast> Visit<'ast> for ModuleDependencyVisitor<'_> {
    fn visit_attribute(&mut self, node: &'ast syn::Attribute) {
        if node.path().is_ident("doc") {
            return;
        }
        visit::visit_attribute(self, node);
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

    /// A macro invocation's arguments are an unparsed token stream, so the
    /// visitor never reaches the paths inside `matches!`, `assert!`, or any
    /// other macro body. Scan the tokens for `crate::`-rooted paths directly.
    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        self.check_token_stream(node.tokens.clone());
        visit::visit_macro(self, node);
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
    fn an_upward_reference_inside_a_macro_body_is_rejected() {
        let files = vec![
            lib(),
            file("crates/coven/src/sync/mod.rs", ""),
            file(
                "crates/coven/src/database/store.rs",
                r#"
                fn is_receipt(object: &Object) -> bool {
                    matches!(object, crate::sync::store::ReclaimObject::Receipt { .. })
                }
                "#,
            ),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].from, "database");
        assert_eq!(violations[0].to, "sync");
    }

    #[test]
    fn an_unassigned_module_fails_the_check() {
        let files = vec![file("crates/coven/src/brand_new_module.rs", "")];
        assert!(find_module_dependency_violations(&files).is_err());
    }

    #[test]
    fn test_sources_and_cfg_test_items_hold_the_same_direction() {
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
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 2);
        assert!(violations
            .iter()
            .all(|violation| violation.from == "protocol" && violation.to == "sync"));
    }

    /// A `<module>_tests` file or directory carries the tests for `<module>`, so
    /// its references answer to that module's region rather than declaring a
    /// region of their own.
    #[test]
    fn a_modules_test_file_answers_to_that_modules_region() {
        let files = vec![
            lib(),
            file("crates/coven/src/sync/mod.rs", ""),
            file("crates/coven/src/handle.rs", ""),
            file("crates/coven/src/database/store.rs", ""),
            file(
                "crates/coven/src/handle_tests/whole_handle.rs",
                "use crate::sync::VerifiedThing;",
            ),
            file(
                "crates/coven/src/database_tests.rs",
                "use crate::sync::VerifiedThing;",
            ),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].from, "database");
        assert_eq!(violations[0].to, "sync");
    }
}
