//! Rust module identities, including modules whose files use `#[path]`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::syntax::{is_test_only, RustFile};

pub(crate) fn module_sources(files: &[RustFile]) -> BTreeMap<String, Vec<String>> {
    let sources =
        files.iter().map(|file| (file.relative_path.as_str(), file)).collect::<BTreeMap<_, _>>();
    let mut modules = BTreeMap::new();
    for file in files.iter().filter(|file| production(file)) {
        let path = Path::new(&file.relative_path);
        if matches!(path.file_name().and_then(|name| name.to_str()), Some("lib.rs" | "main.rs")) {
            let module = inferred_module(file);
            collect(file, module, &sources, &mut modules);
        }
    }
    // Isolated syntax fixtures and feature-specific source files can have no
    // root in the input. Their conventional module path remains well-defined.
    for file in files.iter().filter(|file| production(file)) {
        if !modules.contains_key(&file.relative_path) {
            collect(file, inferred_module(file), &sources, &mut modules);
        }
    }
    modules
}

fn production(file: &RustFile) -> bool {
    !file.is_test_source() && !is_test_only(&file.syntax.attrs)
}

/// The crate is the file's package; the module path is the file's directories
/// beneath the member's source directory (`src/`, or `examples/` and the like),
/// then its stem unless that names the directory's own module.
fn inferred_module(file: &RustFile) -> Vec<String> {
    let components = file
        .relative_path
        .strip_prefix(file.package_root.as_str())
        .expect("a source lies under its member")
        .split('/')
        .collect::<Vec<_>>();
    let mut module = vec![file.package.replace('-', "_")];
    if let [_, directories @ .., _] = components.as_slice() {
        module.extend(directories.iter().map(|part| part.to_string()));
    }
    let stem = components.last().expect("source filename").trim_end_matches(".rs");
    if !matches!(stem, "lib" | "main" | "mod") {
        module.push(stem.to_string());
    }
    module
}

fn collect(
    file: &RustFile,
    module: Vec<String>,
    sources: &BTreeMap<&str, &RustFile>,
    modules: &mut BTreeMap<String, Vec<String>>,
) {
    if modules.insert(file.relative_path.clone(), module.clone()).is_some() {
        return;
    }
    let (directory, child_directory) = directories(file);
    let items = Items {
        path_directory: &directory,
        child_directory: &child_directory,
        file_directory: &directory,
    };
    collect_items(&file.syntax.items, &module, items, sources, modules);
}

/// Where a run of items resolves what it names: `#[path]` attributes against
/// `path_directory`, child module files in `child_directory`, and `include!`
/// against `file_directory`, the directory of the file the items are in.
#[derive(Clone, Copy)]
struct Items<'a> {
    path_directory: &'a Path,
    child_directory: &'a Path,
    file_directory: &'a Path,
}

/// The file an item-position `include!("…")` reads, relative to the
/// directory of the file the call is in. Its items are the including
/// module's. The module declarations among them resolve against the included
/// file's own directory, `#[path]` and child modules alike, as rustc resolves
/// those of a `mod.rs`: `mod case;` in `src/tests/list.rs` is
/// `src/tests/case.rs`, wherever the `include!` is.
fn included_file(item: &syn::Item, file_directory: &Path) -> Option<String> {
    let syn::Item::Macro(item) = item else { return None };
    if !item.mac.path.is_ident("include") {
        return None;
    }
    let path = item.mac.parse_body::<syn::LitStr>().ok()?;
    Some(normalize(&file_directory.join(path.value())))
}

/// Where an included file's items resolve what they name: everything against
/// the file's own directory (`included_file`).
fn included_items(file: &RustFile) -> Items<'_> {
    let directory = Path::new(&file.relative_path).parent().expect("source directory");
    Items { path_directory: directory, child_directory: directory, file_directory: directory }
}

/// The directory a file's `#[path]` attributes are relative to, and the one
/// its child modules' files live in.
fn directories(file: &RustFile) -> (PathBuf, PathBuf) {
    let path = Path::new(&file.relative_path);
    let directory = path.parent().expect("source directory");
    let child_directory = if matches!(
        path.file_stem().and_then(|stem| stem.to_str()),
        Some("lib" | "main" | "mod")
    ) {
        directory.to_path_buf()
    } else {
        directory.join(path.file_stem().expect("source stem"))
    };
    (directory.to_path_buf(), child_directory)
}

/// Where a module declaration's contents are.
enum ModuleContents<'a> {
    /// Inline, in this directory for its own children.
    Inline(&'a [syn::Item], PathBuf),
    /// In whichever of these files exists.
    Files(Vec<String>),
}

fn module_contents<'a>(
    item: &'a syn::ItemMod,
    path_directory: &Path,
    child_directory: &Path,
) -> ModuleContents<'a> {
    let name = item.ident.to_string();
    let explicit = item.attrs.iter().find(|attr| attr.path().is_ident("path")).map(|attr| {
        let syn::Meta::NameValue(value) = &attr.meta else {
            panic!("path attribute must have a value")
        };
        let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(value), .. }) = &value.value else {
            panic!("path attribute must be a string")
        };
        value.value()
    });
    match (&item.content, explicit) {
        (Some((_, items)), explicit) => ModuleContents::Inline(
            items,
            child_directory.join(explicit.as_deref().unwrap_or(&name)),
        ),
        (None, Some(path)) => ModuleContents::Files(vec![normalize(&path_directory.join(path))]),
        (None, None) => ModuleContents::Files(vec![
            normalize(&child_directory.join(format!("{name}.rs"))),
            normalize(&child_directory.join(&name).join("mod.rs")),
        ]),
    }
}

fn collect_items(
    items: &[syn::Item],
    module: &[String],
    directories: Items<'_>,
    sources: &BTreeMap<&str, &RustFile>,
    modules: &mut BTreeMap<String, Vec<String>>,
) {
    let Items { path_directory, child_directory, file_directory } = directories;
    for item in items {
        if let Some(path) = included_file(item, file_directory) {
            if let syn::Item::Macro(macro_item) = item {
                if is_test_only(&macro_item.attrs) {
                    continue;
                }
            }
            if let Some(file) = sources.get(path.as_str()).filter(|file| production(file)) {
                if modules.insert(file.relative_path.clone(), module.to_vec()).is_none() {
                    let items = included_items(file);
                    collect_items(&file.syntax.items, module, items, sources, modules);
                }
            }
            continue;
        }
        let syn::Item::Mod(item) = item else { continue };
        if is_test_only(&item.attrs) {
            continue;
        }
        let mut child = module.to_vec();
        child.push(item.ident.to_string());
        match module_contents(item, path_directory, child_directory) {
            ModuleContents::Inline(items, directory) => {
                let inline = Items {
                    path_directory: &directory,
                    child_directory: &directory,
                    file_directory,
                };
                collect_items(items, &child, inline, sources, modules);
            }
            ModuleContents::Files(paths) => {
                for path in paths {
                    if let Some(file) = sources.get(path.as_str()).filter(|file| production(file)) {
                        collect(file, child.clone(), sources, modules);
                    }
                }
            }
        }
    }
}

/// The files that only test-only module declarations reach, such as one
/// declared `#[cfg(test)] mod tests;`: test code, whatever they are named.
/// A file some production declaration also reaches is production, and one no
/// crate root reaches keeps what its name says.
pub(crate) fn test_module_files(files: &[RustFile]) -> BTreeSet<String> {
    let sources =
        files.iter().map(|file| (file.relative_path.as_str(), file)).collect::<BTreeMap<_, _>>();
    let mut reached = [BTreeSet::new(), BTreeSet::new()];
    for file in files {
        if matches!(
            Path::new(&file.relative_path).file_name().and_then(|name| name.to_str()),
            Some("lib.rs" | "main.rs")
        ) {
            reach(file, false, &sources, &mut reached);
        }
    }
    let [production, test] = reached;
    test.difference(&production).cloned().collect()
}

/// `reached[0]` holds the files reached through production declarations,
/// `reached[1]` those reached through a test-only one.
fn reach(
    file: &RustFile,
    in_test: bool,
    sources: &BTreeMap<&str, &RustFile>,
    reached: &mut [BTreeSet<String>; 2],
) {
    let in_test = in_test || file.is_test_source_by_name() || is_test_only(&file.syntax.attrs);
    if !reached[usize::from(in_test)].insert(file.relative_path.clone()) {
        return;
    }
    let (directory, child_directory) = directories(file);
    let items = Items {
        path_directory: &directory,
        child_directory: &child_directory,
        file_directory: &directory,
    };
    reach_items(&file.syntax.items, in_test, items, sources, reached);
}

fn reach_items(
    items: &[syn::Item],
    in_test: bool,
    directories: Items<'_>,
    sources: &BTreeMap<&str, &RustFile>,
    reached: &mut [BTreeSet<String>; 2],
) {
    let Items { path_directory, child_directory, file_directory } = directories;
    for item in items {
        if let Some(path) = included_file(item, file_directory) {
            let syn::Item::Macro(macro_item) = item else { continue };
            let in_test = in_test || is_test_only(&macro_item.attrs);
            if let Some(file) = sources.get(path.as_str()) {
                let in_test =
                    in_test || file.is_test_source_by_name() || is_test_only(&file.syntax.attrs);
                if reached[usize::from(in_test)].insert(file.relative_path.clone()) {
                    let items = included_items(file);
                    reach_items(&file.syntax.items, in_test, items, sources, reached);
                }
            }
            continue;
        }
        let syn::Item::Mod(item) = item else { continue };
        let in_test = in_test || is_test_only(&item.attrs);
        match module_contents(item, path_directory, child_directory) {
            ModuleContents::Inline(items, directory) => {
                let inline = Items {
                    path_directory: &directory,
                    child_directory: &directory,
                    file_directory,
                };
                reach_items(items, in_test, inline, sources, reached);
            }
            ModuleContents::Files(paths) => {
                for path in paths {
                    if let Some(file) = sources.get(path.as_str()) {
                        reach(file, in_test, sources, reached);
                    }
                }
            }
        }
    }
}

/// Source keys use `/` on every platform (sources.rs converts Windows separators),
/// so a joined path is rebuilt from its components rather than printed natively.
fn normalize(path: &Path) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                parts.pop();
            }
            std::path::Component::CurDir => {}
            part => parts.push(part.as_os_str().to_str().expect("Rust source path is UTF-8")),
        }
    }
    parts.join("/")
}

#[cfg(test)]
#[path = "module_sources_tests.rs"]
mod tests;
