//! Circle calls borrow the open store's connection (E12).

use crate::{
    Audience, Circle, CircleId, CircleMemberInfo, CovenResult, LiveQuery, MemberId, SyncError,
};
use coven_format::admin::StoreChange;

/// A borrowed namespace whose work remains owned by the open store.
/// Changes require online catch-up and report refusals by the applied state
/// as `SyncError::Rejected`. Failed catch-up starts no operation and claims
/// no slot. Lists read the committed state, as their subscriptions do.
pub struct Circles<'a> {
    storage: &'a crate::storage::StorageConnections,
    database: &'a coven_database::Database,
}

impl<'a> Circles<'a> {
    pub(crate) fn new(
        storage: &'a crate::storage::StorageConnections,
        database: &'a coven_database::Database,
    ) -> Self {
        Self { storage, database }
    }
    /// Reset this circle to a snapshot of this device’s state (§19.4).
    pub async fn reset(&self, circle: CircleId) -> Result<(), SyncError> {
        self.storage.reset(Audience::Circle(circle)).await
    }

    /// Make a circle with this member as its first member.
    pub async fn create(&self, name: &str) -> Result<CircleId, SyncError> {
        self.storage.create_circle(name.into()).await
    }
    /// Rename a circle without changing its key, members or rows.
    pub async fn rename(&self, circle: CircleId, name: &str) -> Result<(), SyncError> {
        self.storage
            .administer(StoreChange::RenameCircle {
                circle,
                name: name.into(),
            })
            .await
    }
    /// Claim the circle's deletion; from its slot the deleted-circle rule
    /// hides the circle's rows (§15.9).
    pub async fn delete(&self, circle: CircleId) -> Result<(), SyncError> {
        self.storage
            .administer(StoreChange::DeleteCircle { circle })
            .await
    }
    /// Add a store member, sealing every historical circle key to them.
    pub async fn add_member(&self, circle: CircleId, member: &MemberId) -> Result<(), SyncError> {
        self.storage
            .administer(StoreChange::AddCircleMember {
                circle,
                member: *member,
            })
            .await
    }
    /// Remove a member; remaining members replace the exposed circle key (§15.8).
    pub async fn remove_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<(), SyncError> {
        self.storage
            .administer(StoreChange::RemoveCircleMember {
                circle,
                member: *member,
            })
            .await
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
