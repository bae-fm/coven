//! What the lints' tests share: compiling a fixture crate with the driver, as
//! cargo runs it, and comparing a lint's findings with the lines of the
//! fixture that end in `// finding` or hold `/* finding */`, the form a line
//! that ends in an opening brace takes.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The driver as cargo runs it on the workspace's crates: through its
/// wrapper, which has rustc write each function's MIR into the metadata.
const DRIVER: &str = env!("CARGO_BIN_EXE_typed-lints-rustc");
const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

/// A fixture crate: its source, under `tests/fixtures`, and the folder the
/// driver takes for the crate's own (`CARGO_MANIFEST_DIR`), where the lints
/// look for the workspace's policy file.
pub struct Fixture<'a> {
    pub source: &'a str,
    pub crate_name: &'a str,
    pub manifest_dir: &'a str,
}

impl Fixture<'_> {
    fn source_path(&self) -> PathBuf {
        Path::new(FIXTURES).join(self.source)
    }

    /// Runs the driver on the crate as a library, as cargo runs a workspace
    /// wrapper (`<driver> rustc <args>`), with `extra` arguments.
    pub fn run(
        &self,
        out_dir: &Path,
        extra: impl IntoIterator<Item = impl AsRef<OsStr>>,
    ) -> Output {
        Command::new(DRIVER)
            .args(["rustc", "--edition=2021", "--crate-type=lib", "--error-format=json"])
            .args(["--crate-name", self.crate_name, "--out-dir"])
            .arg(out_dir)
            .arg(self.source_path())
            .args(extra)
            .env("CARGO_MANIFEST_DIR", Path::new(FIXTURES).join(self.manifest_dir))
            .output()
            .expect("the driver runs")
    }

    /// The diagnostics of a compile that succeeds, one JSON object per line.
    pub fn compile(
        &self,
        out_dir: &Path,
        extra: impl IntoIterator<Item = impl AsRef<OsStr>>,
    ) -> String {
        let output = self.run(out_dir, extra);
        let diagnostics = String::from_utf8(output.stderr).expect("rustc writes UTF-8");
        assert!(output.status.success(), "{} did not compile:\n{diagnostics}", self.crate_name);
        diagnostics
    }

    /// Checks that `lint` finds what each line of the fixture that ends in
    /// `// finding` or holds `/* finding */` holds, and nothing else, and that
    /// no mark of the fixture went unfulfilled.
    pub fn assert_findings(&self, lint: &str, diagnostics: &str) {
        let mut found: Vec<u64> = Vec::new();
        for line in diagnostics.lines() {
            let diagnostic: serde_json::Value =
                serde_json::from_str(line).expect("a JSON diagnostic");
            let code = diagnostic["code"]["code"].as_str();
            assert_ne!(
                code,
                Some("unfulfilled_lint_expectations"),
                "the fixture's mark found nothing:\n{line}"
            );
            if code == Some(lint) {
                let primary = diagnostic["spans"]
                    .as_array()
                    .and_then(|spans| spans.iter().find(|span| span["is_primary"] == true))
                    .expect("a finding has a primary span");
                found.push(primary["line_start"].as_u64().expect("a line number"));
            }
        }
        found.sort_unstable();

        let fixture = std::fs::read_to_string(self.source_path()).expect("the fixture reads");
        let expected: Vec<u64> = (1..)
            .zip(fixture.lines())
            .filter(|(_, text)| {
                text.trim_end().ends_with("// finding") || text.contains("/* finding */")
            })
            .map(|(number, _)| number)
            .collect();
        assert_eq!(
            found, expected,
            "the lines that hold findings, then the fixture's:\n{diagnostics}"
        );
    }
}

/// A folder of the test's own for the compiled fixtures.
pub fn out_dir(name: &str) -> PathBuf {
    let out_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    std::fs::create_dir_all(&out_dir).expect("the test's folder is made");
    out_dir
}

/// The arguments that let a fixture use the library `name` compiled into
/// `out_dir`.
pub fn extern_library(name: &str, out_dir: &Path) -> Vec<String> {
    vec![
        "--extern".to_string(),
        format!("{name}={}", out_dir.join(format!("lib{name}.rlib")).display()),
        "-L".to_string(),
        out_dir.to_str().expect("a UTF-8 path").to_string(),
    ]
}
