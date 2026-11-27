//! SQLite's schema cookie as a transaction reads it. The connection compares
//! the cookie with the one it saw when it prepared its kept statements and
//! read the tables' shapes, so a write checks it once for each app statement.
//! A transaction that runs no DDL can't change the cookie, so while a
//! transaction holds the cookie, the connection reads it from SQLite only
//! once.

use super::HostSql;
use crate::authorization::Policy;
use std::cell::Cell;

/// The cookie as a transaction read it. The connection keeps the value until
/// the last holder releases it.
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

/// Holds the cookie for one transaction that runs no DDL. Dropping the last
/// scope clears the stored cookie.
pub struct CookieScope<'a>(&'a SchemaCookie);

impl Drop for CookieScope<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl<P: Policy> HostSql<P> {
    /// Read the schema cookie once until the returned scope drops. The
    /// caller's transaction runs no DDL.
    pub fn keep_schema_cookie(&self) -> CookieScope<'_> {
        self.cookie.hold();
        CookieScope(&self.cookie)
    }

    /// SQLite's count of changes to the main schema. The connection compares
    /// it with the cookie it saw when it prepared its kept statements and
    /// read the tables' shapes.
    pub(super) fn app_schema_cookie(&self) -> rusqlite::Result<i64> {
        if let Some(cookie) = self.cookie.value.get() {
            return Ok(cookie);
        }
        #[cfg(any(test, feature = "test-utils"))]
        self.record_work(|work| work.cookie_reads += 1);
        let cookie = self.schema_cookie()?;
        if self.cookie.holders.get() > 0 {
            self.cookie.value.set(Some(cookie));
        }
        Ok(cookie)
    }
}
