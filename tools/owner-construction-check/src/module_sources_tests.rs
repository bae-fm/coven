use super::*;

#[test]
fn joined_module_paths_match_source_keys_on_every_platform() {
    // Source keys always use `/`; a joined path must too, whatever the native separator.
    let directory = Path::new("crates/coven-storage/src").join("providers");
    assert_eq!(
        normalize(&directory.join("s3_error.rs")),
        "crates/coven-storage/src/providers/s3_error.rs"
    );
    assert_eq!(
        normalize(&directory.join("../mod_tests.rs")),
        "crates/coven-storage/src/mod_tests.rs"
    );
    assert_eq!(
        normalize(&directory.join("./a/../b.rs")),
        "crates/coven-storage/src/providers/b.rs"
    );
}
