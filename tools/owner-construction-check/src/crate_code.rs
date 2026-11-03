//! The code the rules of Appendix F.1 to F.3 read: the files of the members
//! that are not tools, without their test code (`test_code.rs`), with the
//! workspace's own macros expanded (`macro_expansion.rs`), and their SQL
//! files. The database's domains read the crates' test code too, as written.
//! The conventions of F.5 read the files as written instead, tests and tools
//! included.

use crate::finding::Finding;
use crate::macro_expansion::expand_macros;
use crate::policy::Policy;
use crate::sources::{SqlFile, Workspace};
use crate::syntax::RustFile;
use crate::test_code::{production_code, test_code};

pub(crate) struct CrateCode {
    /// The crates' production code as written, macro definitions included.
    pub(crate) written: Vec<RustFile>,
    /// The same code with each invocation of a workspace macro replaced by
    /// what it writes, and the definitions removed.
    pub(crate) expanded: Vec<RustFile>,
    /// Each invocation the checker could not expand.
    pub(crate) unexpanded: Vec<Finding>,
    /// The crates' SQL files outside their `tests/` directories.
    pub(crate) sql: Vec<SqlFile>,
    /// The crates' test code as written (`test_code::test_code`).
    pub(crate) tests: Vec<RustFile>,
    /// The SQL files in the crates' `tests/` directories.
    pub(crate) test_sql: Vec<SqlFile>,
}

impl CrateCode {
    pub(crate) fn read(workspace: &Workspace, policy: &Policy) -> Self {
        let crates = workspace
            .files
            .iter()
            .filter(|file| !policy.is_tool(&file.package))
            .cloned()
            .collect::<Vec<_>>();
        let sql = workspace
            .sql_files
            .iter()
            .filter(|file| !policy.is_tool(&file.package))
            .cloned()
            .collect::<Vec<_>>();
        Self::of(&crates, &sql)
    }

    /// The code of these crate files.
    pub(crate) fn of(files: &[RustFile], sql: &[SqlFile]) -> Self {
        let written = production_code(files);
        let expanded = expand_macros(&written);
        let (test_sql, sql) = sql.iter().cloned().partition(SqlFile::is_test_source);
        Self {
            written,
            expanded: expanded.files,
            unexpanded: expanded.findings,
            sql,
            tests: test_code(files),
            test_sql,
        }
    }
}

/// The code the rules read in these crate files, for a rule's tests to run it
/// as the checker does.
#[cfg(test)]
pub(crate) fn checked(files: &[RustFile]) -> Vec<RustFile> {
    CrateCode::of(files, &[]).expanded
}
