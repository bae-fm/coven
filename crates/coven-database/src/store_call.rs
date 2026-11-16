//! Coven's state for one call on a connection: this device, the
//! transaction's constants and the test records. Coven makes one at the
//! start of each call and passes it beside the connection, so the constants
//! live as long as the call.

use crate::transaction_constants::TransactionConstants;
use coven_foundation::id_source::DeviceId;

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
    /// The callback a test set, which a write calls at each of coven's and
    /// sqlive's write checkpoints it passes (`test_on_write_checkpoint`).
    #[cfg(any(test, feature = "test-utils"))]
    write_checkpoints: std::sync::Arc<std::sync::Mutex<Option<WriteCheckpoints>>>,
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
            #[cfg(any(test, feature = "test-utils"))]
            write_checkpoints: std::sync::Arc::default(),
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

    /// Call `callback` at each point a write on `writer` passes from now on:
    /// sqlive's as it commits, and coven's after.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn on_write_checkpoint(
        &self,
        writer: &crate::sqlite::StoreSql,
        callback: impl Fn(crate::test_utils::WriteCheckpoint) + Send + 'static,
    ) {
        *coven_foundation::locks::lock_unpoisoned(&self.write_checkpoints) =
            Some(Box::new(callback));
        let checkpoints = std::sync::Arc::clone(&self.write_checkpoints);
        writer.on_commit_point(move |point| {
            if let Some(callback) = coven_foundation::locks::lock_unpoisoned(&checkpoints).as_ref()
            {
                callback(point.into());
            }
        });
    }

    /// Tell the test's callback, if any, that a write passed `point`.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn write_checkpoint(&self, point: crate::test_utils::WriteCheckpoint) {
        if let Some(callback) =
            coven_foundation::locks::lock_unpoisoned(&self.write_checkpoints).as_ref()
        {
            callback(point);
        }
    }
}

/// The callback a test sets, which a write calls with each write checkpoint
/// it passes. A callback that panics leaves the slot unchanged, so coven
/// takes the slot's lock even when that panic poisoned it.
#[cfg(any(test, feature = "test-utils"))]
type WriteCheckpoints = Box<dyn Fn(crate::test_utils::WriteCheckpoint) + Send>;

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

    /// Tell the test's callback, if any, that a write passed `point`.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn write_checkpoint(&self, point: crate::test_utils::WriteCheckpoint) {
        self.tests.write_checkpoint(point);
    }

    /// Call `callback` at each point a write on `writer` passes from now on.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn on_write_checkpoint(
        &self,
        writer: &crate::sqlite::StoreSql,
        callback: impl Fn(crate::test_utils::WriteCheckpoint) + Send + 'static,
    ) {
        self.tests.on_write_checkpoint(writer, callback);
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
