use super::*;

impl<'storage> AuthorizedWriterOperation<'storage> {
    #[cfg(test)]
    pub(super) async fn load_own_snapshot_for_test(
        &mut self,
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, snapshot::SnapshotError> {
        self.writer
            .load_own_snapshot(&mut self.history, reference)
            .await
            .map_err(snapshot::SnapshotError::StoreObject)
    }

    #[cfg(test)]
    pub(crate) fn sign_device_head_for_test(
        &self,
        commit: crate::protocol::store_commit::StoreBatchCommitRef,
        history_summary: crate::protocol::store_commit::ObjectHash,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> Result<crate::protocol::store_commit::StoreDeviceHead, StoreError> {
        self.writer.sign_device_head(
            self.store_root().store_root_hash,
            commit,
            history_summary,
            successor,
        )
    }

    #[cfg(test)]
    pub(crate) fn resign_snapshot_meta_for_test(
        &self,
        meta: crate::protocol::store_commit::SnapshotMeta,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, StoreError> {
        if meta.store_root_hash != self.store_root().store_root_hash
            || !self
                .writer
                .is_authored_by_registration(&meta.author_registration)
        {
            return Err(StoreError::InvalidOutbound(
                "snapshot test input belongs to another Store writer".to_string(),
            ));
        }
        self.writer
            .resign_snapshot(meta)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn parse_snapshot_meta_for_test(
        &self,
        bytes: &[u8],
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, StoreError> {
        self.writer
            .parse_snapshot(bytes, self.store_root().store_root_hash, reference)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn local_registration_ref_for_test(
        &self,
    ) -> crate::protocol::store_commit::StoreDeviceRegistrationRef {
        self.writer.registration_reference_for_test()
    }

    #[cfg(test)]
    pub(crate) async fn revoke_member_without_local_adoption_for_test(
        &mut self,
        public_key_hex: &str,
        timestamp: &str,
        current_encryption: &crate::encryption::EncryptionService,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
    ) -> Result<
        crate::encryption::EncryptionService,
        crate::sync::store::membership::MembershipOpsError,
    > {
        self.revoke_member_without_local_adoption(
            public_key_hex,
            timestamp,
            current_encryption,
            pending_rotation,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn complete_revoke_rotation_adoption_for_test(
        &self,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
        adopted_generation: u64,
    ) -> Result<(), crate::sync::store::membership::InviteError> {
        self.complete_revoke_rotation_adoption(pending_rotation, adopted_generation)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn prepare_pending_store_write(&mut self) -> Result<bool, StoreError> {
        self.prepare_store_write().await
    }

    #[cfg(test)]
    pub(crate) async fn drain_store_writes(&mut self) -> Result<u64, StoreError> {
        self.drain_prepared_store_writes().await
    }
}
