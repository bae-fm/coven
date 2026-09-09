use super::*;

pub(super) fn rust_files(root: &Path) -> Result<Vec<RustFile>, CheckError> {
    rust_files_in(root, &[PathBuf::from("crates")])
}

pub(super) fn rust_files_in(
    root: &Path,
    rust_roots: &[PathBuf],
) -> Result<Vec<RustFile>, CheckError> {
    let mut paths = Vec::new();
    for rust_root in rust_roots {
        collect_rust_paths(&root.join(rust_root), &mut paths)?;
    }
    paths.sort();
    paths.dedup();
    paths
        .into_iter()
        .map(|path| {
            let source = std::fs::read_to_string(&path).map_err(|source| CheckError::ReadFile {
                path: path.clone(),
                source,
            })?;
            let syntax = syn::parse_file(&source).map_err(|source| CheckError::ParseFile {
                path: path.clone(),
                source,
            })?;
            let relative_path = path
                .strip_prefix(root)
                .map_err(|source| CheckError::Relativize {
                    path: path.clone(),
                    root: root.to_path_buf(),
                    source,
                })?
                .to_string_lossy()
                .replace('\\', "/");
            Ok(RustFile {
                relative_path,
                syntax,
            })
        })
        .collect()
}

pub(super) fn collect_rust_paths(
    directory: &Path,
    output: &mut Vec<PathBuf>,
) -> Result<(), CheckError> {
    for entry in std::fs::read_dir(directory).map_err(|source| CheckError::ReadDirectory {
        path: directory.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| CheckError::ReadDirectoryEntry {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| {
                matches!(
                    name.to_str(),
                    Some(".claude" | ".codex" | ".git" | "node_modules" | "target")
                )
            }) {
                continue;
            }
            collect_rust_paths(&path, output)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
    Ok(())
}

pub(super) fn collect_structs(files: &[RustFile]) -> BTreeMap<String, StructInfo> {
    let mut structs = BTreeMap::new();
    for file in files {
        for item in &file.syntax.items {
            collect_structs_from_item(item, &mut structs);
        }
    }
    structs
}

pub(super) fn collect_structs_from_item(
    item: &syn::Item,
    structs: &mut BTreeMap<String, StructInfo>,
) {
    match item {
        syn::Item::Struct(item) => {
            let mut field_types = TypeNames::default();
            for field in &item.fields {
                field_types.visit_type(&field.ty);
            }
            structs.insert(
                item.ident.to_string(),
                StructInfo {
                    field_types: field_types.names,
                },
            );
        }
        syn::Item::Mod(item) => {
            if let Some((_, items)) = &item.content {
                for item in items {
                    collect_structs_from_item(item, structs);
                }
            }
        }
        _ => {}
    }
}

pub(super) fn collect_declared_types(files: &[RustFile]) -> BTreeMap<String, StructInfo> {
    let mut types = BTreeMap::new();
    for file in files {
        for item in &file.syntax.items {
            collect_declared_types_from_item(item, &mut types);
        }
    }
    types
}

pub(super) fn collect_declared_types_from_item(
    item: &syn::Item,
    types: &mut BTreeMap<String, StructInfo>,
) {
    let (name, field_types) = match item {
        syn::Item::Struct(item) => {
            let mut names = TypeNames::default();
            for field in &item.fields {
                names.visit_type(&field.ty);
            }
            (Some(item.ident.to_string()), names.names)
        }
        syn::Item::Enum(item) => {
            let mut names = TypeNames::default();
            for variant in &item.variants {
                for field in &variant.fields {
                    names.visit_type(&field.ty);
                }
            }
            (Some(item.ident.to_string()), names.names)
        }
        syn::Item::Type(item) => {
            let mut names = TypeNames::default();
            names.visit_type(&item.ty);
            (Some(item.ident.to_string()), names.names)
        }
        syn::Item::Trait(item) => (Some(item.ident.to_string()), BTreeSet::new()),
        syn::Item::Union(item) => {
            let mut names = TypeNames::default();
            for field in &item.fields.named {
                names.visit_type(&field.ty);
            }
            (Some(item.ident.to_string()), names.names)
        }
        syn::Item::Mod(item) => {
            if let Some((_, items)) = &item.content {
                for item in items {
                    collect_declared_types_from_item(item, types);
                }
            }
            return;
        }
        _ => return,
    };
    let Some(name) = name else {
        return;
    };
    types
        .entry(name)
        .or_insert_with(|| StructInfo {
            field_types: BTreeSet::new(),
        })
        .field_types
        .extend(field_types);
}
