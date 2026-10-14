//! Device-reset notices and the identity check's local reservations (§10, E5).

use super::{Database, DbError};
use crate::{CovenResult, LiveQuery};
use coven_foundation::files::DeviceReset;

pub use crate::device_reset::IdentityReservations;

impl Database {
    /// This database's own reserved counters, read from one snapshot.
    pub async fn identity_reservations(&self) -> Result<IdentityReservations, DbError> {
        self.call(|inner| {
            let device = inner.access.device;
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(|db| crate::device_reset::reservations(db, device))
        })
        .await
    }

    /// Store the replacement notice before the replacement handle is published.
    pub async fn record_device_reset(&self, reset: DeviceReset) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| crate::device_reset::record(db, &reset))
            })
        })
        .await
    }

    /// The latest completed replacement of this installation, or None (E5).
    pub async fn device_reset(&self) -> CovenResult<Option<DeviceReset>> {
        self.read(|sql| sql.device_reset()).await
    }

    /// The same pre-built query, observing committed changes (E4).
    pub fn subscribe_device_reset(&self) -> LiveQuery<Option<DeviceReset>> {
        self.subscribe(|sql| sql.device_reset())
    }

    /// Refuse every later call and live query of this copy: sync decided to
    /// replace it, and nothing it holds may be used or sent again (§10).
    pub fn stop_for_device_reset(&self, reset: DeviceReset) -> Result<(), DbError> {
        let guard = self.inner.read().expect("database lock poisoned");
        let inner = guard.as_ref().ok_or(DbError::StoreClosed)?;
        inner
            .access
            .observer
            .fail(crate::observation::HandleFailure::DeviceReset(reset));
        Ok(())
    }
}
