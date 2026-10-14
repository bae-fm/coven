//! Circle calls borrow the open store's operation owner (E12).

use crate::{Circle, CircleId, CircleMemberInfo, CovenResult, LiveQuery, MemberId, SyncError};

/// A borrowed namespace whose work remains owned by the open store.
/// Changes require online catch-up and use replay's `SyncError::Rejected`
/// reasons. Failed catch-up starts no operation and reserves no entry.
/// Lists read the committed replay, as their subscriptions do.
pub struct Circles<'a> {
    operations: &'a coven_sync::Operations,
    database: &'a coven_database::Database,
}

impl<'a> Circles<'a> {
    pub(crate) fn new(
        operations: &'a coven_sync::Operations,
        database: &'a coven_database::Database,
    ) -> Self {
        Self {
            operations,
            database,
        }
    }
    /// Reset this circle to a snapshot of this device’s state (§19.3).
    pub async fn reset(&self, circle: CircleId) -> Result<(), SyncError> {
        self.operations.reset_circle(circle).await
    }

    /// Make a circle with this member as its first member.
    pub async fn create(&self, name: &str) -> Result<CircleId, SyncError> {
        self.operations.create_circle(name).await
    }
    /// Rename a circle without changing its key, members or rows.
    pub async fn rename(&self, circle: CircleId, name: &str) -> Result<(), SyncError> {
        self.operations.rename_circle(circle, name).await
    }
    /// Delete local rows, publish their write, then delete the circle.
    pub async fn delete(&self, circle: CircleId) -> Result<(), SyncError> {
        self.operations.delete_circle(circle).await
    }
    /// Add a store member, sealing every historical circle key to them.
    pub async fn add_member(&self, circle: CircleId, member: &MemberId) -> Result<(), SyncError> {
        self.operations.add_circle_member(circle, member).await
    }
    /// Remove a member and replace the circle key.
    pub async fn remove_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<(), SyncError> {
        self.operations.remove_circle_member(circle, member).await
    }
    /// The circles this member is in.
    pub async fn list(&self) -> CovenResult<Vec<Circle>> {
        self.database.circles().await
    }
    /// The same circle-list query, live (E4).
    pub fn subscribe_list(&self) -> LiveQuery<Vec<Circle>> {
        self.database.subscribe_circles()
    }
    /// A circle's members who are still in the store.
    pub async fn members(&self, circle: CircleId) -> CovenResult<Vec<CircleMemberInfo>> {
        self.database.circle_members(circle).await
    }
    /// The same query for this circle's members, live (E4).
    pub fn subscribe_members(&self, circle: CircleId) -> LiveQuery<Vec<CircleMemberInfo>> {
        self.database.subscribe_circle_members(circle)
    }
}
