//! SQLite's schema cookie as a transaction reads it. App statements kept
//! prepared and the tables' shapes are judged against the cookie, so a write
//! checks it once for each app statement. A transaction that runs no DDL
//! can't change the cookie, so while one holds it, it is read once.

use super::DatabaseConnection;
use std::cell::Cell;

/// The cookie as one transaction read it, kept while any holder asks.
#[derive(Default)]
pub(super) struct SchemaCookie {
    holders: Cell<usize>,
    value: Cell<Option<i64>>,
}

impl SchemaCookie {
    pub(super) fn hold(&self) {
        self.holders.set(self.holders.get() + 1);
    }

    pub(super) fn release(&self) {
        let holders = self.holders.get().checked_sub(1).expect("a cookie release follows its hold");
        self.holders.set(holders);
        if holders == 0 {
            self.value.set(None);
        }
    }
}

/// Holds the cookie for one transaction that runs no DDL; it goes when the
/// last holder ends.
pub(crate) struct CookieScope<'a>(&'a SchemaCookie);

impl Drop for CookieScope<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl DatabaseConnection {
    /// Read the schema cookie once until the returned scope drops. The
    /// caller's transaction runs no DDL.
    pub(crate) fn keep_schema_cookie(&self) -> CookieScope<'_> {
        self.cookie.hold();
        CookieScope(&self.cookie)
    }

    /// SQLite's count of changes to the main schema, which app statements
    /// kept prepared were judged against and the tables' shapes were read
    /// in.
    pub(super) fn app_schema_cookie(&self) -> rusqlite::Result<i64> {
        if let Some(cookie) = self.cookie.value.get() {
            return Ok(cookie);
        }
        #[cfg(any(test, feature = "test-utils"))]
        self.record_work(|work| work.constant_reads += 1);
        let cookie = self.schema_cookie()?;
        if self.cookie.holders.get() > 0 {
            self.cookie.value.set(Some(cookie));
        }
        Ok(cookie)
    }
}
