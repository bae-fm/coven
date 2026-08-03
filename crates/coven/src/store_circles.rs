use crate::database::StoreDatabase;
use crate::store_membership::StoreMembership;
use crate::store_security::StoreSecurity;
use crate::store_sync::StoreSync;

#[derive(Clone)]
pub(crate) struct StoreCircles {
    database: StoreDatabase,
    membership: StoreMembership,
    security: StoreSecurity,
    sync: StoreSync,
}

impl StoreCircles {
    pub(crate) fn new(
        database: StoreDatabase,
        membership: StoreMembership,
        security: StoreSecurity,
        sync: StoreSync,
    ) -> Self {
        Self {
            database,
            membership,
            security,
            sync,
        }
    }

    async fn query_inputs(
        &self,
    ) -> Result<(String, std::collections::BTreeSet<String>), crate::CircleError> {
        let identity_pubkey = self
            .security
            .established_identity()
            .map_err(|error| crate::CircleError::Identity(error.to_string()))?
            .public_key_hex();
        let store_members = self
            .membership
            .members()
            .await
            .map_err(crate::CircleError::from)?
            .into_iter()
            .map(|member| member.pubkey)
            .collect();
        Ok((identity_pubkey, store_members))
    }

    pub(crate) async fn create(&self, name: &str) -> Result<crate::CircleId, crate::CircleError> {
        self.sync
            .active_circles()?
            .create(name)
            .await
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) async fn install_test_active_circle(
        &self,
        label: &str,
    ) -> Result<crate::CircleId, crate::database::DbError> {
        self.database
            .install_test_active_circle(label.to_string())
            .await
    }

    pub(crate) async fn rename(
        &self,
        circle_id: crate::CircleId,
        name: &str,
    ) -> Result<(), crate::CircleError> {
        self.sync
            .active_circles()?
            .rename(circle_id, name)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn add_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
        role: crate::CircleRole,
    ) -> Result<(), crate::CircleError> {
        self.sync
            .active_circles()?
            .add_member(circle_id, member_pubkey, role)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn remove_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
    ) -> Result<crate::CircleOperationId, crate::CircleError> {
        self.sync
            .active_circles()?
            .remove_member(circle_id, member_pubkey)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn resolve(
        &self,
        circle_id: crate::CircleId,
        chosen: crate::CircleControlCoord,
    ) -> Result<(), crate::CircleError> {
        self.sync
            .active_circles()?
            .resolve(circle_id, chosen)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn cancel_close(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleOperationId, crate::CircleError> {
        self.sync
            .active_circles()?
            .cancel_close(circle_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn exclude_close_device(
        &self,
        circle_id: crate::CircleId,
        device_id: crate::StoreDeviceId,
    ) -> Result<(), crate::CircleError> {
        self.sync
            .active_circles()?
            .exclude_close_device(circle_id, device_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn delete(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<(), crate::CircleError> {
        self.sync
            .active_circles()?
            .delete(circle_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn retry(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::CircleError> {
        self.sync
            .active_circles()?
            .retry(operation_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn discard(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::CircleError> {
        self.sync
            .active_circles()?
            .discard(operation_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn list(&self) -> Result<Vec<crate::Circle>, crate::CircleError> {
        let (identity_pubkey, store_members) = self.query_inputs().await?;
        self.database
            .circle_states(&identity_pubkey, store_members)
            .await
            .map_err(|error| crate::CircleError::Protocol(error.to_string()))
    }

    pub(crate) async fn members(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<Vec<crate::CircleMemberInfo>, crate::CircleError> {
        let (identity_pubkey, store_members) = self.query_inputs().await?;
        self.database
            .get_circle_members(circle_id, &identity_pubkey, store_members)
            .await
            .map_err(|error| crate::CircleError::Protocol(error.to_string()))
    }

    pub(crate) async fn operations(
        &self,
    ) -> Result<Vec<crate::CircleOperationInfo>, crate::CircleError> {
        self.database
            .get_circle_operations()
            .await
            .map_err(|error| crate::CircleError::Protocol(error.to_string()))
    }

    pub(crate) async fn close_status(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleCloseStatus, crate::CircleError> {
        self.sync
            .active_circles()?
            .close_status(circle_id)
            .await
            .map_err(Into::into)
    }
}
