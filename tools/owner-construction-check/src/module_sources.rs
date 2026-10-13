//! Rust module identities, including modules whose files use `#[path]`.

use std::collections::BTreeMap;
use std::path::Path;

use crate::syntax::{is_test_only, is_test_source, RustFile};

pub(crate) fn module_sources(files: &[RustFile]) -> BTreeMap<String, Vec<String>> {
    let sources = files
        .iter()
        .map(|file| (file.relative_path.as_str(), file))
        .collect::<BTreeMap<_, _>>();
    let mut modules = BTreeMap::new();
    for file in files.iter().filter(|file| production(file)) {
        let path = Path::new(&file.relative_path);
        if matches!(
            path.file_name().and_then(|name| name.to_str()),
            Some("lib.rs" | "main.rs")
        ) {
            let module = inferred_module(&file.relative_path);
            collect(file, module, &sources, &mut modules);
        }
    }
    // Isolated syntax fixtures and feature-specific source files can have no
    // root in the input. Their conventional module path remains well-defined.
    for file in files.iter().filter(|file| production(file)) {
        if !modules.contains_key(&file.relative_path) {
            collect(
                file,
                inferred_module(&file.relative_path),
                &sources,
                &mut modules,
            );
        }
    }
    modules
}

fn production(file: &RustFile) -> bool {
    !is_test_source(&file.relative_path) && !is_test_only(&file.syntax.attrs)
}

fn inferred_module(path: &str) -> Vec<String> {
    let components = path.split('/').collect::<Vec<_>>();
    let mut module = vec![components[1].replace('-', "_")];
    module.extend(
        components[3..components.len() - 1]
            .iter()
            .map(|part| part.to_string()),
    );
    let stem = components
        .last()
        .expect("source filename")
        .trim_end_matches(".rs");
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
    if modules
        .insert(file.relative_path.clone(), module.clone())
        .is_some()
    {
        return;
    }
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
    collect_items(
        &file.syntax.items,
        &module,
        directory,
        &child_directory,
        sources,
        modules,
    );
}

fn collect_items(
    items: &[syn::Item],
    module: &[String],
    path_directory: &Path,
    child_directory: &Path,
    sources: &BTreeMap<&str, &RustFile>,
    modules: &mut BTreeMap<String, Vec<String>>,
) {
    for item in items {
        let syn::Item::Mod(item) = item else { continue };
        if is_test_only(&item.attrs) {
            continue;
        }
        let name = item.ident.to_string();
        let mut child = module.to_vec();
        child.push(name.clone());
        let explicit = item
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("path"))
            .map(|attr| {
                let syn::Meta::NameValue(value) = &attr.meta else {
                    panic!("path attribute must have a value")
                };
                let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(value),
                    ..
                }) = &value.value
                else {
                    panic!("path attribute must be a string")
                };
                value.value()
            });
        if let Some((_, items)) = &item.content {
            let directory = child_directory.join(explicit.as_deref().unwrap_or(&name));
            collect_items(items, &child, &directory, &directory, sources, modules);
        } else {
            let paths = match explicit {
                Some(path) => vec![path_directory.join(path)],
                None => vec![
                    child_directory.join(format!("{name}.rs")),
                    child_directory.join(&name).join("mod.rs"),
                ],
            };
            for path in paths {
                let path = normalize(&path);
                if let Some(file) = sources.get(path.as_str()).filter(|file| production(file)) {
                    collect(file, child.clone(), sources, modules);
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
            part => parts.push(
                part.as_os_str()
                    .to_str()
                    .expect("Rust source path is UTF-8"),
            ),
        }
    }
    parts.join("/")
}

#[cfg(test)]
#[path = "module_sources_tests.rs"]
mod tests;
