//! Circle calls borrow the open store's handle (E12).

use crate::{
    Audience, Circle, CircleId, CircleMemberInfo, CovenHandle, CovenResult, LiveQuery, MemberId,
    OperationId, SyncError,
};

/// A borrowed namespace whose work remains owned by the open store. Making a
/// circle and each change is its `CovenWrite` step in a write of its own,
/// returning the operation's id; a change returns `None` when the applied
/// state already has what it asks for (§11.1). Lists read the committed state, as their subscriptions do.
pub struct Circles<'a> {
    /// The handle whose writes the changes are steps of.
    handle: &'a CovenHandle,
    storage: &'a crate::storage::StorageConnections,
    database: &'a coven_database::Database,
}

impl<'a> Circles<'a> {
    pub(crate) fn new(
        handle: &'a CovenHandle,
        storage: &'a crate::storage::StorageConnections,
        database: &'a coven_database::Database,
    ) -> Self {
        Self { handle, storage, database }
    }
    /// Reset this circle to a snapshot of this device’s state (§19.4).
    pub async fn reset(&self, circle: CircleId) -> Result<(), SyncError> {
        self.storage.reset(Audience::Circle(circle)).await
    }

    /// Make a circle (`CovenWrite::create_circle`).
    pub async fn create(&self, name: &str) -> Result<(CircleId, OperationId), SyncError> {
        let name = name.to_owned();
        self.handle
            .write(move |write| Ok(write.create_circle(&name)?))
            .await
            .map_err(SyncError::from)
    }
    /// Rename a circle (`CovenWrite::rename_circle`).
    pub async fn rename(
        &self,
        circle: CircleId,
        name: &str,
    ) -> Result<Option<OperationId>, SyncError> {
        let name = name.to_owned();
        self.handle
            .write(move |write| Ok(write.rename_circle(circle, &name)?))
            .await
            .map_err(SyncError::from)
    }
    /// Delete a circle (`CovenWrite::delete_circle`).
    pub async fn delete(&self, circle: CircleId) -> Result<Option<OperationId>, SyncError> {
        self.handle
            .write(move |write| Ok(write.delete_circle(circle)?))
            .await
            .map_err(SyncError::from)
    }
    /// Add a store member to a circle (`CovenWrite::add_circle_member`).
    pub async fn add_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<Option<OperationId>, SyncError> {
        let member = *member;
        self.handle
            .write(move |write| Ok(write.add_circle_member(circle, &member)?))
            .await
            .map_err(SyncError::from)
    }
    /// Remove a member from a circle (`CovenWrite::remove_circle_member`).
    pub async fn remove_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<Option<OperationId>, SyncError> {
        let member = *member;
        self.handle
            .write(move |write| Ok(write.remove_circle_member(circle, &member)?))
            .await
            .map_err(SyncError::from)
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
