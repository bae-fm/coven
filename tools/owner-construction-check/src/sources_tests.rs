use super::*;

/// A fresh directory under the system's temporary directory, removed when dropped.
struct Fixture(PathBuf);

impl Fixture {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir()
            .join(format!("owner-construction-check-{name}-{}", std::process::id()));
        // A run that stopped early may have left its fixture; none is fine too.
        match std::fs::remove_dir_all(&root) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                panic!("remove a leftover fixture: {error}")
            }
            Ok(()) | Err(_) => {}
        }
        Self(root)
    }

    fn write(&self, path: &str, contents: &str) {
        let path = self.0.join(path);
        std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture directory");
        std::fs::write(path, contents).expect("write fixture file");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove fixture workspace");
    }
}

/// Members at the workspace root, as in a workspace without a `crates/`
/// directory: each file knows its member, and its member's `tests/` alone
/// holds integration tests.
#[test]
fn members_anywhere_give_their_files_a_package() {
    let fixture = Fixture::new("members");
    fixture.write(
        "Cargo.toml",
        "[workspace]\nmembers = [\"app-core\", \"app-util/\"]\n[workspace.dependencies]\nutil = { path = \"app-util\", package = \"app-util\" }\n",
    );
    fixture.write(
        "app-core/Cargo.toml",
        "[package]\nname = \"app-core\"\n[dependencies]\nutil.workspace = true\n",
    );
    fixture.write("app-core/src/lib.rs", "mod db;");
    fixture.write("app-core/src/db/tests/bench.rs", "");
    fixture.write("app-core/tests/flow.rs", "");
    fixture.write("app-util/Cargo.toml", "[package]\nname = \"app-util\"\n");
    fixture.write("app-util/build.rs", "fn main() {}");
    fixture.write("app-ignored/src/lib.rs", "");
    let workspace = load(&fixture.0).expect("load the fixture workspace");
    let files = workspace
        .files
        .iter()
        .map(|file| {
            (
                file.relative_path.as_str(),
                file.package_root.as_str(),
                file.package.as_str(),
                file.is_test_source(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        files,
        [
            ("app-core/src/db/tests/bench.rs", "app-core/", "app-core", false),
            ("app-core/src/lib.rs", "app-core/", "app-core", false),
            ("app-core/tests/flow.rs", "app-core/", "app-core", true),
            ("app-util/build.rs", "app-util/", "app-util", false),
        ]
    );
    assert_eq!(
        workspace.files[1].extern_crates,
        BTreeMap::from([("util".to_string(), "app_util".to_string())])
    );
    assert_eq!(
        workspace
            .manifests
            .iter()
            .map(|manifest| manifest.relative_path.as_str())
            .collect::<Vec<_>>(),
        ["app-core/Cargo.toml", "app-util/Cargo.toml"]
    );
}

#[test]
fn members_are_directories_that_hold_one_package_each() {
    for (members, files) in [
        ("[\"crates/*\"]", &[][..]),
        ("[\"app\", \"app/inner\"]", &[("app/Cargo.toml", "[package]\nname = \"app\"")][..]),
        (
            "[\"app\"]",
            &[("app/Cargo.toml", "[package]\nname = \"app\""), ("app/fuzz/Cargo.toml", "")][..],
        ),
        ("[\"app\"]", &[("app/Cargo.toml", "[dependencies]")][..]),
        ("[\"app\"]", &[("app/src/lib.rs", "")][..]),
    ] {
        let fixture = Fixture::new("refused");
        fixture.write("Cargo.toml", &format!("[workspace]\nmembers = {members}\n"));
        for (path, contents) in files {
            fixture.write(path, contents);
        }
        assert!(load(&fixture.0).is_err(), "{members} {files:?}");
    }
}
