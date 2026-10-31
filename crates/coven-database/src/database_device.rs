//! Device-reset notices and committed member lists (§19.3, E5).

use super::{Database, DbError};
use crate::{CovenResult, LiveQuery, NewOperation, PendingValue, ProviderChanges};
use coven_foundation::files::DeviceReset;

impl Database {
    /// Commit carried provider cleanup with the replacement notice before
    /// publication. A resumed bootstrap cannot import the same work twice.
    pub async fn record_device_reset<P: PendingValue>(
        &self,
        reset: DeviceReset,
        operations: Vec<NewOperation>,
        provider: ProviderChanges,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    if crate::device_reset::read(db)? == Some(reset) {
                        return Ok(());
                    }
                    for operation in &operations {
                        crate::operation::insert(db, operation)?;
                    }
                    provider.record::<P>(db)?;
                    crate::device_reset::record(db, &reset)
                })
            })
        })
        .await
    }

    /// This installation's device id, or `StoreClosed` once closed.
    pub(crate) fn local_device(&self) -> Result<coven_foundation::id_source::DeviceId, DbError> {
        let guard = self.inner.read().expect("database lock poisoned");
        Ok(guard.as_ref().ok_or(DbError::StoreClosed)?.access.device)
    }

    /// Store members and every device they registered, from the committed
    /// sequence prefix (E9). `subscribe_members` runs the same query.
    pub async fn members(&self) -> CovenResult<Vec<crate::MemberInfo>> {
        let device = self.local_device()?;
        self.read(move |sql| sql.members(device)).await
    }

    /// The same members query, live (E4).
    pub fn subscribe_members(&self) -> LiveQuery<Vec<crate::MemberInfo>> {
        let device = self.local_device().ok();
        self.subscribe(move |sql| sql.members(device.ok_or(DbError::StoreClosed)?))
    }

    /// The circles this installation's member belongs to (E12).
    pub async fn circles(&self) -> CovenResult<Vec<crate::Circle>> {
        let device = self.local_device()?;
        self.read(move |sql| sql.circles(device)).await
    }

    /// The same circle-list query, live (E4).
    pub fn subscribe_circles(&self) -> LiveQuery<Vec<crate::Circle>> {
        let device = self.local_device().ok();
        self.subscribe(move |sql| sql.circles(device.ok_or(DbError::StoreClosed)?))
    }

    /// A circle's members who remain in the store; none for an absent or
    /// deleted circle (E12).
    pub async fn circle_members(
        &self,
        circle: coven_foundation::id_source::CircleId,
    ) -> CovenResult<Vec<crate::CircleMemberInfo>> {
        let device = self.local_device()?;
        self.read(move |sql| sql.circle_members(device, circle)).await
    }

    /// The same circle-members query, live (E4).
    pub fn subscribe_circle_members(
        &self,
        circle: coven_foundation::id_source::CircleId,
    ) -> LiveQuery<Vec<crate::CircleMemberInfo>> {
        let device = self.local_device().ok();
        self.subscribe(move |sql| sql.circle_members(device.ok_or(DbError::StoreClosed)?, circle))
    }

    /// The latest completed replacement of this installation, or None (E5).
    pub async fn device_reset(&self) -> CovenResult<Option<DeviceReset>> {
        self.read(|sql| sql.device_reset()).await
    }

    /// The same pre-built query, observing committed changes (E4).
    pub fn subscribe_device_reset(&self) -> LiveQuery<Option<DeviceReset>> {
        self.subscribe(|sql| sql.device_reset())
    }
}

#[cfg(test)]
#[path = "database_device_tests.rs"]
mod tests;
