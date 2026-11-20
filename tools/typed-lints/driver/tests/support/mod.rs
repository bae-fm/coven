//! What the lints' tests share: compiling a fixture crate with the driver, as
//! cargo runs it, and comparing a lint's findings with the lines of the
//! fixture that end in `// finding` or hold `/* finding */`, the form a line
//! that ends in an opening brace takes. A fixture that several lints check
//! names the lints instead, without `clippy::`, in the comment that ends the
//! line: `// read_filter read_aggregate`.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The driver as cargo runs it on the workspace's crates: through its
/// wrapper, which has rustc write each function's MIR into the metadata.
const DRIVER: &str = env!("CARGO_BIN_EXE_typed-lints-rustc");
pub const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

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

    /// The arguments of a compile of the crate as a library, with `extra`
    /// arguments, as cargo hands them to rustc.
    pub fn arguments(
        &self,
        out_dir: &Path,
        extra: impl IntoIterator<Item = impl AsRef<OsStr>>,
    ) -> Vec<OsString> {
        let mut arguments = ["--edition=2021", "--crate-type=lib", "--error-format=json"]
            .into_iter()
            .chain(["--crate-name", self.crate_name, "--out-dir"])
            .map(OsString::from)
            .collect::<Vec<_>>();
        arguments.push(out_dir.into());
        arguments.push(self.source_path().into());
        arguments.extend(extra.into_iter().map(|argument| argument.as_ref().to_owned()));
        arguments
    }

    /// Runs the driver with `arguments` after `rustc`, as cargo runs a
    /// workspace wrapper (`<driver> rustc <args>`).
    pub fn run_with(&self, arguments: impl IntoIterator<Item = impl AsRef<OsStr>>) -> Output {
        Command::new(DRIVER)
            .arg("rustc")
            .args(arguments)
            .env("CARGO_MANIFEST_DIR", Path::new(FIXTURES).join(self.manifest_dir))
            .output()
            .expect("the driver runs")
    }

    /// Runs the driver on the crate as a library, with `extra` arguments.
    pub fn run(
        &self,
        out_dir: &Path,
        extra: impl IntoIterator<Item = impl AsRef<OsStr>>,
    ) -> Output {
        self.run_with(self.arguments(out_dir, extra))
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
        assert_findings_marked(lint, diagnostics, &[self.source]);
    }
}

/// Checks that `lint` finds, in the compile that wrote `diagnostics`, what
/// each line of the fixture sources `marked` that ends in `// finding` or
/// holds `/* finding */` holds, once for each `/* finding */` it holds, and
/// what each line whose last comment names the lint (`// read_sort`) holds;
/// and nothing else; and that no mark went unfulfilled.
pub fn assert_findings_marked(lint: &str, diagnostics: &str, marked: &[&str]) {
    let mut found: Vec<(String, u64)> = Vec::new();
    for line in diagnostics.lines() {
        let diagnostic: serde_json::Value = serde_json::from_str(line).expect("a JSON diagnostic");
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
            let file = primary["file_name"].as_str().expect("a file name");
            found.push((
                marked_source(file, marked),
                primary["line_start"].as_u64().expect("a line number"),
            ));
        }
    }
    found.sort_unstable();

    let name = lint.trim_start_matches("clippy::");
    let mut expected: Vec<(String, u64)> = Vec::new();
    for source in marked {
        let fixture =
            std::fs::read_to_string(Path::new(FIXTURES).join(source)).expect("the fixture reads");
        for (number, text) in (1..).zip(fixture.lines()) {
            let count = text.matches("/* finding */").count()
                + text.rsplit_once("// ").map_or(0, |(_, comment)| {
                    comment.split_whitespace().filter(|word| *word == name).count()
                })
                + usize::from(text.trim_end().ends_with("// finding"));
            expected.extend(std::iter::repeat_n((source.to_string(), number), count));
        }
    }
    expected.sort_unstable();
    assert_eq!(found, expected, "the lines that hold findings, then the fixture's:\n{diagnostics}");
}

/// <path> with `/` as its only separator and no `\\?\` prefix. On Windows,
/// rustc and cargo write one path with both separators (a module's file
/// after the folder it was given with `/`, as `dependencies/src\database.rs`;
/// the dep-info's `D:\a\coven/tests\reads`) and some with the `\\?\`
/// prefix, so a comparison of paths reads each through this.
pub fn slash_path(path: &str) -> String {
    let path = path.replace('\\', "/");
    path.strip_prefix("//?/").map_or(path.clone(), str::to_string)
}

/// The fixture source of <marked> that the file <file> of a diagnostic is, or
/// <file> itself when it is none, read through `slash_path`.
pub fn marked_source(file: &str, marked: &[&str]) -> String {
    let file = slash_path(file);
    marked
        .iter()
        .find(|source| file.ends_with(*source))
        .map_or(file.clone(), |source| source.to_string())
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
