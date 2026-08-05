use super::*;

impl LocalStoreWriter {
    #[cfg(test)]
    pub(crate) fn sign_circle_commit_for_test(
        &self,
        old_commit: &crate::protocol::store_commit::StoreBatchCommit,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        reference: crate::protocol::store_commit::CircleControlRef,
        stream_activations: Vec<crate::protocol::store_commit::StreamActivation>,
    ) -> Result<
        crate::protocol::store_commit::StoreBatchCommit,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        if old_commit.author_registration != *self.registration.reference() {
            return Err(
                crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                    "test Circle commit is not authored by the local writer".to_string(),
                ),
            );
        }
        self.sign_circle_commit(
            old_commit.store_root_hash,
            old_commit.write_id.clone(),
            coord,
            old_commit.order.clone(),
            old_commit.membership_state.clone(),
            old_commit.device_state.clone(),
            old_commit
                .operations_membership_authority()
                .map_err(|error| {
                    crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                        format!(
                            "prepared Circle commit has no validated operations authority: {error}"
                        ),
                    )
                })?,
            reference,
            stream_activations,
        )
    }

    #[cfg(test)]
    pub(crate) fn resign_store_commit_for_test(
        &self,
        commit: &mut crate::protocol::store_commit::StoreBatchCommit,
    ) {
        commit.resign(&self.device_signer);
    }

    #[cfg(test)]
    pub(crate) async fn load_own_snapshot(
        &self,
        history: &mut crate::sync::store::owner::writer::AuthorizedStoreHistory<'_>,
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        crate::protocol::store_commit::SnapshotMeta,
        crate::protocol::objects::StoreObjectError,
    > {
        history
            .load_store_snapshot(
                self.registration.reference(),
                self.registration.value(),
                reference,
            )
            .await
            .map(|(_, meta)| meta)
    }

    #[cfg(test)]
    pub(crate) fn resign_snapshot(
        &self,
        meta: crate::protocol::store_commit::SnapshotMeta,
    ) -> Result<
        crate::protocol::store_commit::SnapshotMeta,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::SnapshotMeta::signed(
            meta.store_root_hash,
            self.registration.reference().clone(),
            meta.generation,
            meta.predecessor.clone(),
            meta.image.clone(),
            meta.coverage.clone(),
            meta.state.clone(),
            meta.history_summary.clone(),
            meta.schema_version,
            meta.created_at.clone(),
            meta.successor.clone(),
            &self.device_signer,
        )
    }

    #[cfg(test)]
    pub(crate) fn parse_snapshot(
        &self,
        bytes: &[u8],
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        crate::protocol::store_commit::SnapshotMeta,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::SnapshotMeta::parse_at(
            bytes,
            store_root_hash,
            reference,
            self.registration.value(),
        )
    }

    #[cfg(test)]
    pub(crate) fn parse_snapshot_stream_entry(
        &self,
        bytes: &[u8],
        root: &crate::protocol::store_commit::StoreRootRef,
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        crate::protocol::store_commit::SnapshotMeta,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::SnapshotMeta::parse_stream_entry_at(
            bytes,
            root,
            self.registration.reference(),
            self.registration.value(),
            reference,
        )
    }

    #[cfg(test)]
    pub(crate) fn registration_reference_for_test(
        &self,
    ) -> crate::protocol::store_commit::StoreDeviceRegistrationRef {
        self.registration.reference().clone()
    }
}
