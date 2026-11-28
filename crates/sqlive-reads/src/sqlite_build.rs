//! The SQLite build that the compiler checks reads against and the app runs
//! them on, which must be the same so that a read the build accepts prepares
//! in the app.

use std::ffi::CString;
use std::fmt;

/// The expression depth limit coven builds SQLite with
/// (`SQLITE_MAX_EXPR_DEPTH`). SQLite refuses a statement that reads a WITH
/// table after it has opened as many tables as this limit, and each name of a
/// WITH table or a view opens that table's tables again, MATERIALIZED or not.
/// It also refuses an expression that nests deeper than this limit.
pub const EXPRESSION_DEPTH_LIMIT: u32 = 4_000;

/// The SQLite this process runs was built with another expression depth
/// limit than [`EXPRESSION_DEPTH_LIMIT`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OtherExpressionDepthLimit;

impl fmt::Display for OtherExpressionDepthLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SQLite was built with another expression depth limit than coven's \
             (SQLITE_MAX_EXPR_DEPTH={EXPRESSION_DEPTH_LIMIT} in LIBSQLITE3_FLAGS)"
        )
    }
}

impl std::error::Error for OtherExpressionDepthLimit {}

/// Refuses a SQLite built with another expression depth limit than
/// [`EXPRESSION_DEPTH_LIMIT`].
pub fn expression_depth_limit_built() -> Result<(), OtherExpressionDepthLimit> {
    let option = CString::new(format!("MAX_EXPR_DEPTH={EXPRESSION_DEPTH_LIMIT}"))
        .expect("the option holds no NUL");
    // SAFETY: the option is a NUL-terminated string that outlives the call.
    if unsafe { rusqlite::ffi::sqlite3_compileoption_used(option.as_ptr()) } == 0 {
        return Err(OtherExpressionDepthLimit);
    }
    Ok(())
}
