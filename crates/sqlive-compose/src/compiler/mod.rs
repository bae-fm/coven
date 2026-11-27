//! The compiler an app's build script runs: it finds a crate's `fragment!`,
//! `read!` and `write!` calls, checks each against the app's schema through sqlive,
//! and writes the Rust their macros include (`SPEC.md`, section 10).
//!
//! The compiler reads no file and no environment variable. It takes a
//! crate's sources, its dependencies' manifests and the schema's setup as
//! values, and returns the files, the manifest and the warnings as values.

mod columns;
mod core;
mod crate_compile;
mod crate_report;
mod error;
mod generate;
mod header;
mod json;
mod lex;
mod model;
mod rename;
mod setup;
mod source;
mod spread;
mod unit;
mod write;

use std::io;

pub use crate_compile::compile_crate;
pub use crate_report::CompiledCall;
pub use error::{Cause, CompileError, Location, Warning, WholePlace};
pub use header::{CallKind, HeaderError};
pub use setup::{Function, Setup};

/// What `compile_crate` compiles.
pub struct Input<'a> {
    /// The crate's name.
    pub crate_name: String,
    /// The text of the file at a path below the crate's folder.
    pub source_reader: &'a dyn Fn(&str) -> io::Result<String>,
    /// The manifest of each crate it depends on directly that writes
    /// fragments, as text.
    pub manifests: Vec<String>,
    /// The schema's setup.
    pub setup: Setup,
}

/// What a crate compiles to.
#[derive(Debug)]
pub struct Output {
    /// Each generated file's name and text.
    pub files: Vec<(String, String)>,
    /// The crate's own manifest, as text.
    pub manifest: String,
    /// Each file the source reader gave, by its path.
    pub read: Vec<String>,
    /// The warnings of the crate's reads.
    pub warnings: Vec<Warning>,
    /// Each `fragment!`, `read!` and `write!` call the compiler found and compiled, in
    /// the order the walk finds them: the crate's modules from `src/lib.rs`,
    /// leaving out the modules that `#[cfg(test)]` marks.
    pub calls: Vec<CompiledCall>,
}
