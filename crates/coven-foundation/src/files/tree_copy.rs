//! Tests' copies of an installation's directory tree, as a backup or a
//! crash leaves it.

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
