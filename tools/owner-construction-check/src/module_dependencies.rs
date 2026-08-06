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
//!         └──────────► coven-protocol ──► coven-keys ──► coven-foundation
//! ```
//!
//! Every top-level module of the coven crate is assigned to a region, and a
//! `crate::` reference from one region to another must point down the diagram.
//! Within-region references are unrestricted — this gate asserts the direction
//! between regions, not the structure inside one.
//!
//! A region that has become a crate of its own leaves this table: for an
//! *upward* reference Cargo's own dependency graph is then the gate, and it is
//! the stronger one, because it forbids the reference at resolution rather than
//! by inspection. So the table shrinks as the workspace splits, and holds
//! exactly the regions still sharing the `coven` crate.
//!
//! Cargo does not settle the *sibling* direction, though: an edge between two
//! extracted crates is not a cycle, so nothing stops one from simply declaring
//! the other as a dependency. Extracted crates therefore stay under this gate —
//! their sources are read for references to the other extracted crates, checked
//! against the same rank and sibling rules the module form answers to.
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
///
/// A region whose every module has become a crate leaves this enum along with
/// its table rows — `coven-foundation`, `coven-keys`, and `coven-protocol` sit
/// below everything here, and Cargo will not resolve a reference back up into
/// `coven`. What remains ranks the regions still sharing the `coven` crate.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum Region {
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

/// A region whose modules have left `coven` for a crate of their own, keyed by
/// that crate's name. Cargo stops a reference *up* out of the extracted crate,
/// but it does not stop a module still inside `coven` from naming it, nor one
/// extracted crate from declaring another as a dependency — so the sibling and
/// rank rules keep applying to paths rooted at these names, both in `coven` and
/// in the extracted crates' own sources.
pub(crate) const EXTRACTED_CRATE_REGIONS: &[(&str, Region)] = &[
    ("coven_database", Region::Database),
    ("coven_storage", Region::Storage),
    ("coven_replication", Region::Replication),
];

/// Database and Storage are siblings: replication composes both, but neither
/// may reach into the other. Replication outranks both, so it may name either.
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
        let (from, from_region, resolves_crate_paths) = match file_origin(&file.relative_path) {
            Some(Origin::Module(module)) => {
                let module = if known_modules.contains(&module) {
                    module
                } else if let Some(subject) = module
                    .strip_suffix("_tests")
                    .filter(|subject| known_modules.contains(*subject))
                {
                    subject.to_string()
                } else {
                    continue;
                };
                let region = regions[module.as_str()];
                (module, region, true)
            }
            Some(Origin::ExtractedCrate(name, region)) => (name.to_string(), region, false),
            None => continue,
        };
        let mut visitor = ModuleDependencyVisitor {
            path: &file.relative_path,
            from: &from,
            from_region,
            resolves_crate_paths,
            regions: &regions,
            known_modules: &known_modules,
            reexports: &reexports,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    Ok(violations.into_iter().collect())
}

/// What gives a scanned file its region.
enum Origin {
    /// A top-level module of the `coven` crate.
    Module(String),
    /// A file inside a crate that has been extracted out of `coven`, which the
    /// whole crate's region covers.
    ExtractedCrate(&'static str, Region),
}

/// The referencing file's region source: its top-level module when the file is
/// still inside `coven` — the first path component under `crates/coven/src/`,
/// or the file stem for root-level files — otherwise the extracted crate it
/// belongs to. `coven`'s `lib.rs` is exempt: it is the composition surface that
/// names every module.
fn file_origin(path: &str) -> Option<Origin> {
    if let Some(relative) = path.strip_prefix("crates/coven/src/") {
        let top = relative.split('/').next()?;
        let module = top.strip_suffix(".rs").unwrap_or(top);
        if module == "lib" {
            return None;
        }
        return Some(Origin::Module(module.to_string()));
    }
    EXTRACTED_CRATE_REGIONS
        .iter()
        .find(|(name, _)| path.starts_with(&format!("crates/{}/src/", name.replace('_', "-"))))
        .map(|(name, region)| Origin::ExtractedCrate(name, *region))
}

/// The top-level module of a file still inside `coven`.
fn file_module(path: &str) -> Option<String> {
    match file_origin(path)? {
        Origin::Module(module) => Some(module),
        Origin::ExtractedCrate(..) => None,
    }
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
    from_region: Region,
    /// Whether a `crate::`-rooted path names one of `coven`'s modules. Inside an
    /// extracted crate it names that crate's own modules — its internal
    /// structure, which this gate does not rank — so only the crate-name form is
    /// checked there.
    resolves_crate_paths: bool,
    regions: &'a BTreeMap<&'static str, Region>,
    known_modules: &'a BTreeSet<String>,
    reexports: &'a BTreeMap<String, String>,
    violations: &'a mut BTreeSet<ModuleDependencyViolation>,
}

impl ModuleDependencyVisitor<'_> {
    /// `segments` is a `crate::`-rooted path with the `crate` segment removed.
    fn check_reference(&mut self, segments: &[String], line: usize) {
        if !self.resolves_crate_paths {
            return;
        }
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
        let from_region = self.from_region;
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

    /// A path rooted at an extracted crate's name, checked against the same
    /// rank and sibling rules the module form answers to.
    fn check_extracted_crate_reference(&mut self, crate_name: &str, line: usize) {
        let Some(to_region) = EXTRACTED_CRATE_REGIONS
            .iter()
            .find(|(name, _)| *name == crate_name)
            .map(|(_, region)| *region)
        else {
            return;
        };
        if crate_name == self.from {
            return;
        }
        let from_region = self.from_region;
        if allows(from_region, to_region) {
            return;
        }
        self.violations.insert(ModuleDependencyViolation {
            path: self.path.to_string(),
            line,
            from: self.from.to_string(),
            to: crate_name.to_string(),
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
            match segments.first() {
                Some(first) if first == "crate" => {
                    self.check_reference(&segments[1..], node.span().start().line);
                }
                Some(first) => {
                    self.check_extracted_crate_reference(first, node.span().start().line);
                }
                None => {}
            }
        }
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        let segments: Vec<String> = node
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect();
        match segments.first() {
            Some(first) if first == "crate" => {
                self.check_reference(&segments[1..], node.span().start().line);
            }
            Some(first) if node.segments.len() > 1 => {
                self.check_extracted_crate_reference(first, node.span().start().line);
            }
            _ => {}
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
            "pub use joining::JoinRequestCode;",
        )
    }

    #[test]
    fn an_upward_reference_is_rejected() {
        let files = vec![
            lib(),
            file(
                "crates/coven/src/joining/mod.rs",
                "use crate::handle::CovenHandle;",
            ),
            file("crates/coven/src/handle.rs", ""),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].from, "joining");
        assert_eq!(violations[0].to, "handle");
    }

    #[test]
    fn a_downward_reference_is_allowed() {
        let files = vec![
            lib(),
            file(
                "crates/coven/src/joining/mod.rs",
                "use coven_replication::sync::store::Store;",
            ),
        ];
        assert!(find_module_dependency_violations(&files)
            .expect("check runs")
            .is_empty());
    }

    /// Cargo would resolve an edge between the two extracted sibling crates —
    /// it is not a cycle — so this gate is what forbids it, in both directions.
    #[test]
    fn the_extracted_siblings_may_not_reach_each_other() {
        let files = vec![
            lib(),
            file(
                "crates/coven-storage/src/remote.rs",
                "use coven_database::StoreDatabase;",
            ),
            file(
                "crates/coven-database/src/store.rs",
                "use coven_storage::CloudHome;",
            ),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 2);
        assert_eq!(violations[0].from, "coven_database");
        assert_eq!(violations[0].to, "coven_storage");
        assert_eq!(violations[1].from, "coven_storage");
        assert_eq!(violations[1].to, "coven_database");
    }

    /// Replication composes both siblings, so naming either from replication is
    /// the allowed direction.
    #[test]
    fn replication_may_name_the_crates_below_it() {
        let files = vec![
            lib(),
            file(
                "crates/coven-replication/src/sync/mod.rs",
                "use coven_database::StoreDatabase;\nuse coven_storage::CloudHome;",
            ),
        ];
        assert!(find_module_dependency_violations(&files)
            .expect("check runs")
            .is_empty());
    }

    /// Naming replication from a crate replication depends on is a reference
    /// up. Cargo refuses that one as a cycle; the rank rule rejects it here
    /// first, naming the direction rather than the resolution failure.
    #[test]
    fn the_crates_below_replication_may_not_name_it() {
        let files = vec![
            lib(),
            file(
                "crates/coven-database/src/store.rs",
                "use coven_replication::sync::store::Store;",
            ),
            file(
                "crates/coven-storage/src/remote.rs",
                "use coven_replication::blob::transition::LocalBlobTransitions;",
            ),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 2);
        assert_eq!(violations[0].from, "coven_database");
        assert_eq!(violations[0].to, "coven_replication");
        assert_eq!(violations[1].from, "coven_storage");
        assert_eq!(violations[1].to, "coven_replication");
    }

    /// `crate::` inside an extracted crate names that crate's own modules. They
    /// are its internal structure, not `coven`'s regions, even when a name is
    /// shared.
    #[test]
    fn a_crate_rooted_path_inside_an_extracted_crate_is_its_own() {
        let files = vec![
            lib(),
            file("crates/coven/src/joining/mod.rs", ""),
            file(
                "crates/coven-storage/src/remote.rs",
                "use crate::joining::SomethingOfItsOwn;",
            ),
        ];
        assert!(find_module_dependency_violations(&files)
            .expect("check runs")
            .is_empty());
    }

    #[test]
    fn a_root_reexport_cannot_bypass_the_gate() {
        let files = vec![
            file("crates/coven/src/lib.rs", "pub use handle::CovenHandle;"),
            file("crates/coven/src/handle.rs", ""),
            file(
                "crates/coven/src/joining/mod.rs",
                "fn open() { let _ = crate::CovenHandle::open(\"p\"); }",
            ),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].to, "handle");
    }

    #[test]
    fn an_upward_reference_inside_a_macro_body_is_rejected() {
        let files = vec![
            lib(),
            file("crates/coven/src/handle.rs", ""),
            file(
                "crates/coven/src/joining/mod.rs",
                r#"
                fn is_open(handle: &Handle) -> bool {
                    matches!(handle, crate::handle::CovenHandle::Open { .. })
                }
                "#,
            ),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].from, "joining");
        assert_eq!(violations[0].to, "handle");
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
            file("crates/coven/src/handle.rs", ""),
            file(
                "crates/coven/src/joining_tests.rs",
                "use crate::handle::CovenHandle;",
            ),
            file(
                "crates/coven/src/joining/mod.rs",
                r#"
                #[cfg(test)]
                mod tests { use crate::handle::CovenHandle; }
                "#,
            ),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 2);
        assert!(violations
            .iter()
            .all(|violation| violation.from == "joining" && violation.to == "handle"));
    }

    /// A `<module>_tests` file or directory carries the tests for `<module>`, so
    /// its references answer to that module's region rather than declaring a
    /// region of their own.
    #[test]
    fn a_modules_test_file_answers_to_that_modules_region() {
        let files = vec![
            lib(),
            file("crates/coven/src/joining/mod.rs", ""),
            file("crates/coven/src/handle.rs", ""),
            file(
                "crates/coven/src/handle_tests/whole_handle.rs",
                "use crate::joining::JoinRequestCode;",
            ),
            file(
                "crates/coven/src/joining_tests.rs",
                "use crate::handle::CovenHandle;",
            ),
        ];
        let violations = find_module_dependency_violations(&files).expect("check runs");
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].from, "joining");
        assert_eq!(violations[0].to, "handle");
    }
}
