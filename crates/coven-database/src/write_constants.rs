//! What a local write, or applying a slot, reads of coven's state that the
//! transaction can't change: the schema version, SQLite's schema cookie and
//! each synced column's and foreign key's registered id. Neither runs a
//! migration or DDL, so each is read once in it rather than once for every
//! row or statement; outside them each is read afresh. The transaction's own
//! authored parts are kept too, changed only where it records them
//! (write_sources.rs).

use super::DatabaseConnection;
use crate::DbError;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct WriteConstants {
    active: Cell<bool>,
    schema_version: Cell<Option<u32>>,
    schema_cookie: Cell<Option<i64>>,
    columns: RefCell<HashMap<(String, String), i64>>,
    foreign_keys: RefCell<HashMap<(String, Vec<u8>), i64>>,
    parts: RefCell<HashMap<PartKey, Option<AuthoredPart>>>,
}

/// An authored part's write id, as coven stores it, and audience.
type PartKey = (Vec<u8>, String);

/// An authored part's header and whether it is complete.
pub(crate) type AuthoredPart = (Vec<u8>, bool);

/// Holds the constants for one local write; they go when it ends.
pub(crate) struct ConstantScope<'a>(&'a WriteConstants);

impl Drop for ConstantScope<'_> {
    fn drop(&mut self) {
        self.0.active.set(false);
        self.0.schema_version.set(None);
        self.0.schema_cookie.set(None);
        self.0.columns.borrow_mut().clear();
        self.0.foreign_keys.borrow_mut().clear();
        self.0.parts.borrow_mut().clear();
    }
}

impl DatabaseConnection {
    /// Read each constant once until the returned scope drops. The caller's
    /// transaction runs no migration and no DDL.
    pub(crate) fn write_constants(&self) -> ConstantScope<'_> {
        self.constants.active.set(true);
        ConstantScope(&self.constants)
    }

    /// The app schema version the store is at.
    pub(crate) fn schema_version(&self) -> Result<u32, DbError> {
        if let Some(version) = self.constants.schema_version.get() {
            return Ok(version);
        }
        #[cfg(any(test, feature = "test-utils"))]
        self.record_work(|work| work.constant_reads += 1);
        let version = self.query_row("PRAGMA user_version", [], |r| {
            r.get::<_, i32>(0).map(|v| v as u32)
        })?;
        if self.constants.active.get() {
            self.constants.schema_version.set(Some(version));
        }
        Ok(version)
    }

    /// SQLite's count of changes to the main schema, which app statements
    /// kept prepared were judged against.
    pub(super) fn app_schema_cookie(&self) -> rusqlite::Result<i64> {
        if let Some(cookie) = self.constants.schema_cookie.get() {
            return Ok(cookie);
        }
        #[cfg(any(test, feature = "test-utils"))]
        self.record_work(|work| work.constant_reads += 1);
        let cookie = {
            let _scope = self.authorization.internal();
            self.connection
                .prepare_cached("PRAGMA main.schema_version")?
                .query_row([], |r| r.get::<_, i64>(0))?
        };
        if self.constants.active.get() {
            self.constants.schema_cookie.set(Some(cookie));
        }
        Ok(cookie)
    }

    /// The registered id of a synced column. Preparing the schema registers
    /// every one, so a cell or live loss can only name a registered column
    /// (Appendix B.1).
    pub(crate) fn column_id(&self, table: &str, column: &str) -> Result<i64, DbError> {
        let key = (table.to_owned(), column.to_owned());
        if let Some(id) = self.constants.columns.borrow().get(&key) {
            return Ok(*id);
        }
        #[cfg(any(test, feature = "test-utils"))]
        self.record_work(|work| work.constant_reads += 1);
        let id: Option<i64> = self.query_row(
            "SELECT (SELECT id FROM _coven_columns WHERE table_name=?1 AND column_name=?2)",
            rusqlite::params![table, column],
            |r| r.get(0),
        )?;
        let id = id.unwrap_or_else(|| panic!("column {column} of {table} was never registered"));
        if self.constants.active.get() {
            self.constants.columns.borrow_mut().insert(key, id);
        }
        Ok(id)
    }

    /// The registered id of a synced table's foreign key, by its encoded
    /// identity, or none when it was never registered.
    pub(crate) fn foreign_key_id(
        &self,
        table: &str,
        identity: &[u8],
    ) -> Result<Option<i64>, DbError> {
        let key = (table.to_owned(), identity.to_vec());
        if let Some(id) = self.constants.foreign_keys.borrow().get(&key) {
            return Ok(Some(*id));
        }
        #[cfg(any(test, feature = "test-utils"))]
        self.record_work(|work| work.constant_reads += 1);
        let id: Option<i64> = self.query_row(
            "SELECT (SELECT id FROM _coven_foreign_keys WHERE table_name=?1 AND identity=?2)",
            rusqlite::params![table, identity],
            |r| r.get(0),
        )?;
        if let Some(id) = id.filter(|_| self.constants.active.get()) {
            self.constants.foreign_keys.borrow_mut().insert(key, id);
        }
        Ok(id)
    }

    /// The authored part of `write` in `audience`, as `_coven_authored_parts`
    /// holds it.
    pub(crate) fn authored_part(
        &self,
        write: &[u8],
        audience: &str,
    ) -> Result<Option<AuthoredPart>, DbError> {
        let key = (write.to_vec(), audience.to_owned());
        if let Some(part) = self.constants.parts.borrow().get(&key) {
            return Ok(part.clone());
        }
        let part = self
            .query(
                "SELECT header,complete FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2",
                rusqlite::params![write, audience],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, bool>(1)?)),
            )?
            .pop();
        if self.constants.active.get() {
            self.constants.parts.borrow_mut().insert(key, part.clone());
        }
        Ok(part)
    }

    /// The authored part now holds `part`, or none.
    pub(crate) fn authored_part_changed(
        &self,
        write: &[u8],
        audience: &str,
        part: Option<AuthoredPart>,
    ) {
        if self.constants.active.get() {
            self.constants
                .parts
                .borrow_mut()
                .insert((write.to_vec(), audience.to_owned()), part);
        }
    }

    /// The authored part may have changed: read it afresh next time.
    pub(crate) fn forget_authored_part(&self, write: &[u8], audience: &str) {
        self.constants
            .parts
            .borrow_mut()
            .remove(&(write.to_vec(), audience.to_owned()));
    }
}

#[cfg(test)]
#[path = "write_constants_tests.rs"]
mod tests;
