//! Circle calls borrow the open store's operation owner (E12).

use crate::{Circle, CircleId, CircleMemberInfo, MemberId, SyncError};

/// A borrowed namespace whose work remains owned by the open store.
pub struct Circles<'a> {
    operations: &'a coven_sync::Operations,
}

impl<'a> Circles<'a> {
    pub(crate) fn new(operations: &'a coven_sync::Operations) -> Self {
        Self { operations }
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
    /// The circles this member currently belongs to.
    pub async fn list(&self) -> Result<Vec<Circle>, SyncError> {
        self.operations.circles().await
    }
    /// Circle members who remain active in the store.
    pub async fn members(&self, circle: CircleId) -> Result<Vec<CircleMemberInfo>, SyncError> {
        self.operations.circle_members(circle).await
    }
}
