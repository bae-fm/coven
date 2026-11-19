//! `clippy::reqwest_error_display` run by the driver, as cargo runs it, over
//! `fixtures/reqwest_error_display.rs`: the lint finds what each line ending
//! in `// finding` prints, and nothing else, and the fixture's mark holds.

use std::path::{Path, PathBuf};
use std::process::Command;

const DRIVER: &str = env!("CARGO_BIN_EXE_typed-lints-driver");
const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

/// Compiles `source` as a library crate with the driver, as cargo runs a
/// workspace wrapper (`<driver> rustc <args>`), and returns its diagnostics,
/// one JSON object per line.
fn compile(source: &str, name: &str, out_dir: &Path, extra: &[&str]) -> String {
    let output = Command::new(DRIVER)
        .args(["rustc", "--edition=2021", "--crate-type=lib", "--error-format=json"])
        .args(["--crate-name", name, "--out-dir"])
        .arg(out_dir)
        .arg(Path::new(FIXTURES).join(source))
        .args(extra)
        .output()
        .expect("the driver runs");
    let diagnostics = String::from_utf8(output.stderr).expect("rustc writes UTF-8");
    assert!(output.status.success(), "{name} did not compile:\n{diagnostics}");
    diagnostics
}

#[test]
fn finds_each_printed_reqwest_error_and_nothing_else() {
    let out_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("reqwest_error_display");
    std::fs::create_dir_all(&out_dir).expect("the test's folder is made");
    compile("reqwest.rs", "reqwest", &out_dir, &["--cap-lints=allow"]);
    let extern_reqwest = format!("reqwest={}", out_dir.join("libreqwest.rlib").display());
    let diagnostics = compile(
        "reqwest_error_display.rs",
        "reqwest_error_display",
        &out_dir,
        &["--extern", &extern_reqwest, "-L", out_dir.to_str().expect("a UTF-8 path")],
    );

    let mut found: Vec<u64> = Vec::new();
    for line in diagnostics.lines() {
        let diagnostic: serde_json::Value = serde_json::from_str(line).expect("a JSON diagnostic");
        let code = diagnostic["code"]["code"].as_str();
        assert_ne!(
            code,
            Some("unfulfilled_lint_expectations"),
            "the fixture's mark found nothing:\n{line}"
        );
        if code == Some("clippy::reqwest_error_display") {
            let primary = diagnostic["spans"]
                .as_array()
                .and_then(|spans| spans.iter().find(|span| span["is_primary"] == true))
                .expect("a finding has a primary span");
            found.push(primary["line_start"].as_u64().expect("a line number"));
        }
    }
    found.sort_unstable();

    let fixture = std::fs::read_to_string(Path::new(FIXTURES).join("reqwest_error_display.rs"))
        .expect("the fixture reads");
    let expected: Vec<u64> = (1..)
        .zip(fixture.lines())
        .filter(|(_, text)| text.trim_end().ends_with("// finding"))
        .map(|(number, _)| number)
        .collect();
    assert_eq!(found, expected, "the lines that hold findings, then the fixture's:\n{diagnostics}");
}
