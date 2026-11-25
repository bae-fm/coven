//! The issue tracker of `SPEC.md`: its schema, and a way to compile a crate
//! of it from sources held in memory.
//!
//! A panic here fails the test that called it.

use std::collections::BTreeMap;

use crate::compiler::{compile_crate, CompileError, Function, Input, Output, Setup};

/// The tables of the tracker.
pub const SCHEMA: &str = "
CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, photo BLOB NOT NULL) STRICT;
CREATE TABLE projects (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL) STRICT;
CREATE TABLE issues (
    id TEXT NOT NULL PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects (id),
    number INTEGER NOT NULL,
    title TEXT NOT NULL,
    state TEXT NOT NULL,
    author_id TEXT NOT NULL REFERENCES people (id),
    assignee_id TEXT REFERENCES people (id),
    opened_at INTEGER NOT NULL
) STRICT;
CREATE TABLE labels (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, hidden INTEGER NOT NULL) STRICT;
CREATE TABLE issue_labels (
    issue_id TEXT NOT NULL REFERENCES issues (id),
    label_id TEXT NOT NULL REFERENCES labels (id),
    PRIMARY KEY (issue_id, label_id)
) STRICT;
CREATE TABLE comments (
    id TEXT NOT NULL PRIMARY KEY,
    issue_id TEXT NOT NULL REFERENCES issues (id),
    author_id TEXT NOT NULL REFERENCES people (id),
    body TEXT NOT NULL,
    written_at INTEGER NOT NULL
) STRICT;
CREATE TABLE comment_reads (
    issue_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    read_at INTEGER NOT NULL,
    PRIMARY KEY (issue_id, person_id)
) STRICT;";

/// The tracker's setup step.
pub fn setup() -> Setup {
    Setup {
        sql: vec![SCHEMA.to_owned()],
        functions: vec![Function { name: "fold_case".into(), arguments: 1, deterministic: true }],
        collations: vec![],
    }
}

/// Compile the crate `name` whose files are `files`, given the manifests of
/// the crates it depends on.
pub fn compile(
    name: &str,
    files: &[(&str, &str)],
    manifests: &[&str],
) -> Result<Output, Vec<CompileError>> {
    let files: BTreeMap<String, String> =
        files.iter().map(|(path, text)| ((*path).to_owned(), (*text).to_owned())).collect();
    let reader = move |path: &str| {
        files.get(path).cloned().ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
    };
    compile_crate(Input {
        crate_name: name.to_owned(),
        source_reader: &reader,
        manifests: manifests.iter().map(|manifest| (*manifest).to_owned()).collect(),
        setup: setup(),
    })
}

/// The compile of a crate whose `src/lib.rs` is `lib`.
pub fn compile_lib(lib: &str) -> Result<Output, Vec<CompileError>> {
    compile("tracker", &[("src/lib.rs", lib)], &[])
}

/// The error messages of a failed compile.
pub fn messages(result: Result<Output, Vec<CompileError>>) -> Vec<String> {
    match result {
        Ok(_) => panic!("the compile passes"),
        Err(errors) => errors.iter().map(ToString::to_string).collect(),
    }
}
