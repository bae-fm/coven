//! Coven's state for one call on a connection: this device, the
//! transaction's constants and the test records. Coven makes one at the
//! start of each call and passes it beside the connection, so the constants
//! live as long as the call.

use crate::sqlite::DatabaseConnection;
use crate::transaction_constants::TransactionConstants;
use crate::DbError;
use coven_foundation::id_source::DeviceId;
use std::sync::Arc;

/// Coven's state for one call on a connection, beside the connection.
pub(crate) struct StoreCall<'a> {
    /// This device, on the store's writer once it observes commits; none on
    /// every other connection.
    device: Option<DeviceId>,
    constants: TransactionConstants,
    #[cfg_attr(
        not(any(test, feature = "test-utils")),
        expect(dead_code, reason = "only tests read what coven's work records")
    )]
    tests: &'a TestRecords,
}

/// What coven's tests read back of a store's work across calls: each queued
/// write's record and judgement, the merge's loads of each table, and the
/// counter coven counts its own work in. Coven's connections share one; in a
/// build without tests it holds nothing.
pub(crate) struct TestRecords {
    #[cfg(any(test, feature = "test-utils"))]
    work: std::sync::Mutex<Option<crate::test_utils::WorkCounter>>,
    /// Each queued write's record and judgement, also of a refused one, for
    /// tests of its classification.
    #[cfg(test)]
    judged: std::sync::Mutex<Vec<(coven_format::write::WriteRecord, crate::judge::Judgement)>>,
    #[cfg(test)]
    merge_loads: std::sync::Mutex<std::collections::BTreeMap<String, usize>>,
}

impl TestRecords {
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(any(test, feature = "test-utils"))]
            work: std::sync::Mutex::new(None),
            #[cfg(test)]
            judged: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            merge_loads: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    /// Count coven's work in `counter` from now on.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn track_work(&self, counter: crate::test_utils::WorkCounter) {
        *coven_foundation::locks::lock_unpoisoned(&self.work) = Some(counter);
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn record_work(&self, update: impl FnOnce(&mut crate::test_utils::StoreWork)) {
        if let Some(counter) = coven_foundation::locks::lock_unpoisoned(&self.work).as_ref() {
            counter.record(update);
        }
    }
}

/// Run `run` with the state of one call on a connection that serves no open
/// store's calls: no device, and test records of its own.
pub(crate) fn outside_store<R>(run: impl FnOnce(&StoreCall<'_>) -> R) -> R {
    run(&StoreCall::new(None, &TestRecords::new()))
}

impl<'a> StoreCall<'a> {
    pub(crate) fn new(device: Option<DeviceId>, tests: &'a TestRecords) -> Self {
        Self { device, constants: TransactionConstants::default(), tests }
    }

    pub(crate) fn constants(&self) -> &TransactionConstants {
        &self.constants
    }

    pub(crate) fn local_device(&self) -> Option<DeviceId> {
        self.device
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn record_work(&self, update: impl FnOnce(&mut crate::test_utils::StoreWork)) {
        self.tests.record_work(update);
    }

    /// The last queued write's record and judgement, also of a refused one.
    #[cfg(test)]
    pub(crate) fn take_judged(
        &self,
    ) -> Option<(coven_format::write::WriteRecord, crate::judge::Judgement)> {
        coven_foundation::locks::lock_unpoisoned(&self.tests.judged).pop()
    }

    #[cfg(test)]
    pub(crate) fn judged(
        &self,
        judged: (coven_format::write::WriteRecord, crate::judge::Judgement),
    ) {
        coven_foundation::locks::lock_unpoisoned(&self.tests.judged).push(judged);
    }

    #[cfg(test)]
    pub(crate) fn record_merge_load(&self, table: &str) {
        *coven_foundation::locks::lock_unpoisoned(&self.tests.merge_loads)
            .entry(table.into())
            .or_default() += 1;
    }

    #[cfg(test)]
    pub(crate) fn merge_loads(&self) -> std::collections::BTreeMap<String, usize> {
        coven_foundation::locks::lock_unpoisoned(&self.tests.merge_loads).clone()
    }
}

/// Refuse a queued value before allocating it, using `sql`'s limit.
pub(crate) fn check_value_length(
    sql: &DatabaseConnection,
    field: &'static str,
    length: u64,
) -> Result<usize, DbError> {
    let maximum = sql.length_limit()? as u64;
    if length > maximum {
        return Err(DbError::TooLarge { field, actual: length, maximum });
    }
    Ok(length as usize)
}

pub(crate) fn upload_plaintext<'s>(
    sql: &'s DatabaseConnection,
    rowid: i64,
) -> Result<crate::UploadBytes<'s>, DbError> {
    Ok(crate::UploadBytes::new(sql.open_blob("_coven_uploads", "record", rowid, true)?))
}

pub(crate) fn pending_change<T>(
    sql: &DatabaseConnection,
    run: impl FnOnce() -> Result<T, DbError>,
) -> Result<T, DbError> {
    run().map_err(|error| pending_failure(sql, error))
}

/// A transaction on `sql` that saves pending state: a failure fails the
/// handle, so every later call reports it.
pub(crate) fn state_transaction<T>(
    sql: &mut DatabaseConnection,
    run: impl FnOnce(&DatabaseConnection) -> Result<T, DbError>,
) -> Result<T, DbError> {
    let result = sql.transaction(run);
    result.map_err(|error| pending_failure(sql, error))
}

pub(crate) fn lost_values(sql: &DatabaseConnection) -> Result<Vec<crate::LostValue>, DbError> {
    let records = sql.query(
        &format!("{} ORDER BY l.id", crate::loss_record::SELECT),
        [],
        crate::loss_record::read,
    )?;
    records.into_iter().map(crate::LostValue::from_record).collect()
}

/// Mark ordinary materialization SQL so shared triggers stay suppressed.
/// SQLite retains ownership of foreign keys, actions and deferred checks.
pub(crate) fn materialize<T>(
    sql: &DatabaseConnection,
    run: impl FnOnce(&DatabaseConnection) -> Result<T, DbError>,
) -> Result<T, DbError> {
    let _scope = sql.applying();
    run(sql)
}

/// Fail the handle `sql` serves when saving pending state failed, so every
/// later call reports it.
fn pending_failure(sql: &DatabaseConnection, error: DbError) -> DbError {
    if matches!(error, DbError::StatePersistence(_)) {
        return error;
    }
    let error = Arc::new(error);
    sql.fail_handle(crate::store_failure::StoreFailure::State(error.clone()));
    DbError::StatePersistence(error)
}
