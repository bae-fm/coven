//! The two kinds of handle a host opens a database with, which a compiled
//! read's type names (SPEC.md section 4): the host's run methods take the
//! reads of their own kind alone.

/// The handle that writes a database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Writer {}

/// A handle that only reads a database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadOnly {}
