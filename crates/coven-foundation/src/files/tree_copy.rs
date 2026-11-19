//! Tests' copies of an installation's directory tree, as a backup or a
//! crash leaves it, and the list of the files in one.

use std::path::Path;

/// Copy the tree at `from` into `to`, making `to` and every folder below
/// it, as restoring a backup of an installation does.
pub fn copy_tree(from: &Path, to: &Path) {
    copy_tree_keeping(from, to, &|_| true);
}

/// Copy the tree at `from` into `to`, as `copy_tree` does, with only the
/// files whose name `keep` accepts.
pub fn copy_tree_keeping(from: &Path, to: &Path, keep: &dyn Fn(&str) -> bool) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().expect("a test file's name is Unicode");
        let target = to.join(&name);
        if entry.file_type().unwrap().is_dir() {
            copy_tree_keeping(&entry.path(), &target, keep);
        } else if keep(&name) {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Every file in the tree at `dir`, at any depth, sorted by path. Folders are
/// not listed.
pub fn files_under(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut folders = vec![dir.to_path_buf()];
    while let Some(folder) = folders.pop() {
        for entry in std::fs::read_dir(&folder).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                folders.push(entry.path());
            } else {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    files
}
