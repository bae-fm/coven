use super::*;

impl LocalStoreWriter {
    pub(crate) fn verify_circle_roster_head(
        &self,
        head: &coven_protocol::circle::CircleRosterHead,
    ) -> bool {
        head.verify_for_registration(self.registration.value())
    }

    pub(crate) fn verify_circle_metadata_head(
        &self,
        head: &coven_protocol::circle::CircleMetadataHead,
    ) -> bool {
        head.verify_for_registration(self.registration.value())
    }

    pub(crate) fn sign_circle_roster_head(
        &self,
        entry: &coven_protocol::circle::CircleRosterEntry,
        tip: coven_protocol::objects::ExactObjectRef,
        successor: coven_protocol::store_commit::SuccessorLink,
    ) -> coven_protocol::circle::CircleRosterHead {
        coven_protocol::circle::CircleRosterHead::signed(entry, tip, successor, &self.device_signer)
    }

    pub(crate) fn sign_circle_metadata_head(
        &self,
        metadata: &coven_protocol::circle::CircleMetadata,
        tip: coven_protocol::objects::ExactObjectRef,
        successor: coven_protocol::store_commit::SuccessorLink,
    ) -> coven_protocol::circle::CircleMetadataHead {
        coven_protocol::circle::CircleMetadataHead::signed(
            metadata,
            tip,
            successor,
            &self.device_signer,
        )
    }

    pub(crate) fn sign_circle_control_head(
        &self,
        control: &coven_protocol::circle::CircleControl,
        entry: coven_protocol::objects::ExactObjectRef,
        successor: coven_protocol::store_commit::SuccessorLink,
    ) -> coven_protocol::circle::CircleControlHead {
        coven_protocol::circle::CircleControlHead::signed(
            control,
            entry,
            successor,
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finalize_circle_epoch_close(
        &self,
        candidate_family: coven_protocol::store_commit::CandidateFamilyId,
        metadata_stamp: &str,
        store_membership: coven_protocol::circle_control::StoreMembershipStateRef,
        membership_authority: coven_protocol::membership::MembershipGrantCreationAuthority,
        store_members: Vec<(String, coven_protocol::membership::MemberRole)>,
        close_control: &coven_protocol::circle::PreparedCircleControl,
        current_roster: &coven_protocol::circle::CircleMaterializedRoster,
        current_roster_chain: coven_protocol::circle::CircleRosterChain,
        current_metadata: &coven_protocol::circle::CircleMetadata,
        keyring: &str,
        intent: coven_protocol::circle::CircleEpochCloseIntent,
        responses: Vec<coven_protocol::circle::CircleEpochCloseSettlement>,
        ids: &dyn coven_foundation::id_provider::IdProvider,
    ) -> Result<
        coven_protocol::circle::CircleTransitionDraft,
        coven_protocol::circle::CircleTransitionError,
    > {
        coven_protocol::circle::CircleTransitionDraft::finalize_epoch_close(
            candidate_family,
            &self.circle_device_id(),
            self.registration.reference(),
            metadata_stamp,
            store_membership,
            membership_authority,
            store_members,
            close_control,
            current_roster,
            current_roster_chain,
            current_metadata,
            keyring,
            intent,
            responses,
            ids,
            self,
        )
    }

    pub(crate) fn sign_circle_epoch_close_exclusion(
        &self,
        control: &coven_protocol::circle::PreparedCircleControl,
        excluded: coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        coven_protocol::circle::CircleEpochCloseExclusion,
        coven_protocol::circle::CircleTransitionError,
    > {
        coven_protocol::circle::CircleEpochCloseExclusion::signed(control, excluded, &self.identity)
    }

    pub(crate) async fn load_circle_activations(
        &self,
        history: &mut crate::sync::store::owner::circles::VerifiedCircleHistory<'_, '_>,
        verified: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<
        crate::sync::store::circle_controls::VerifiedCircleActivations,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        history
            .activations()
            .load(verified, &self.identity, routing_key)
            .await
    }

    pub(crate) fn circle_ack_semantic_prefix(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        sequence: u64,
    ) -> String {
        coven_protocol::store_commit::circle_ack_slot_prefix(
            circle_id,
            &self.registration.value().device_id.to_string(),
            sequence,
        )
    }

    pub(crate) fn circle_ack_first_slot(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<coven_protocol::objects::ObjectSlot, coven_protocol::objects::StorageError> {
        coven_protocol::objects::ObjectSlot::logical(format!(
            "{}.json",
            self.circle_ack_semantic_prefix(circle_id, 1)
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_circle_acknowledgement(
        &self,
        root_hash: coven_protocol::store_commit::ObjectHash,
        circle_id: coven_protocol::circle::CircleId,
        sequence: u64,
        frontier: coven_protocol::store_commit::CommitFrontier,
        control: coven_protocol::circle::CircleControlCoord,
        epoch_id: coven_protocol::circle::CircleEpochId,
        key_fingerprint: coven_keys::encryption::KeyFingerprint,
        seeded_from: Option<coven_protocol::circle::CircleBootstrapCoverageRef>,
        sync_time: String,
        predecessor: Option<coven_protocol::objects::ExactObjectRef>,
        next_slot: coven_protocol::objects::ObjectSlot,
    ) -> Result<
        coven_protocol::store_commit::CircleAck,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        let activation = coven_protocol::store_commit::StreamActivation::device_authorized(
            root_hash,
            self.registration.reference().clone(),
            coven_protocol::store_commit::DeviceStreamAnchor::CircleAcknowledgements {
                circle_id,
                first_slot: self.circle_ack_first_slot(circle_id).map_err(|error| {
                    coven_protocol::store_commit::StoreProtocolError::Malformed(error.to_string())
                })?,
            },
        )
        .activation_id();
        coven_protocol::store_commit::CircleAck::signed(
            root_hash,
            circle_id,
            self.registration.reference().clone(),
            sequence,
            frontier,
            control,
            epoch_id,
            key_fingerprint,
            seeded_from,
            sync_time,
            coven_protocol::store_commit::SuccessorLink {
                activation,
                predecessor,
                next_slot,
            },
            &self.device_signer,
        )
    }

    pub(crate) fn local_circle_close_participant(
        &self,
        close: &coven_protocol::circle::CircleEpochClose,
    ) -> Option<coven_protocol::circle::CircleEpochCloseParticipant> {
        close
            .participants
            .iter()
            .find(|participant| participant.registration == *self.registration.reference())
            .cloned()
    }

    pub(crate) fn sign_circle_epoch_close_response(
        &self,
        control: &coven_protocol::circle::PreparedCircleControl,
        frontier: coven_protocol::store_commit::CommitFrontier,
    ) -> Result<
        coven_protocol::circle::CircleEpochCloseResponse,
        coven_protocol::circle::CircleTransitionError,
    > {
        coven_protocol::circle::CircleEpochCloseResponse::signed(
            control,
            self.registration.reference().clone(),
            frontier,
            self.registration.value(),
            &self.device_signer,
        )
    }

    pub(crate) fn verify_local_circle_epoch_close_response(
        &self,
        response: &coven_protocol::circle::CircleEpochCloseResponse,
        control: &coven_protocol::circle::PreparedCircleControl,
    ) -> bool {
        response.verify_for(control, self.registration.value())
    }

    pub(crate) fn circle_snapshot_semantic_prefix(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        generation: u64,
    ) -> String {
        coven_protocol::store_commit::circle_snapshot_slot_prefix(
            circle_id,
            &self.registration.value().device_id.to_string(),
            generation,
        )
    }

    pub(crate) fn circle_snapshot_image_semantic_prefix(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        image_hash: coven_protocol::store_commit::ObjectHash,
    ) -> String {
        coven_protocol::store_commit::circle_snapshot_image_semantic_prefix(
            circle_id,
            &self.registration.value().device_id.to_string(),
            image_hash,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_circle_snapshot_meta(
        &self,
        root_hash: coven_protocol::store_commit::ObjectHash,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
        epoch_id: coven_protocol::circle::CircleEpochId,
        key_fingerprint: coven_keys::encryption::KeyFingerprint,
        generation: u64,
        bootstrap: coven_protocol::circle::CircleBootstrapRef,
        created_at: String,
        predecessor: Option<coven_protocol::store_commit::CircleSnapshotRef>,
        next_slot: coven_protocol::objects::ObjectSlot,
    ) -> Result<
        coven_protocol::store_commit::CircleSnapshotMeta,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        let activation = coven_protocol::store_commit::circle_snapshot_stream_activation(
            root_hash,
            self.registration.reference(),
            circle_id,
            &self.registration.value().device_id.to_string(),
        )?;
        coven_protocol::store_commit::CircleSnapshotMeta::signed(
            root_hash,
            circle_id,
            self.registration.reference().clone(),
            control,
            epoch_id,
            key_fingerprint,
            generation,
            bootstrap,
            created_at,
            coven_protocol::store_commit::CircleSnapshotSuccessorLink {
                activation,
                predecessor,
                next_slot,
            },
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_circle_commit(
        &self,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        write_id: coven_protocol::write::WriteId,
        coord: coven_protocol::store_commit::StoreCommitCoord,
        order: coven_protocol::store_commit::StoreCommitOrder,
        membership_state: coven_protocol::circle_control::StoreMembershipStateRef,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
        membership_authority: coven_protocol::store_commit::StoreOperationMembershipAuthority,
        circle_reference: coven_protocol::store_commit::CircleControlRef,
        stream_activations: Vec<coven_protocol::store_commit::StreamActivation>,
    ) -> Result<
        coven_protocol::store_commit::StoreBatchCommit,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        self.sign_store_write_commit(
            store_root_hash,
            write_id,
            coord,
            order,
            membership_state,
            device_state,
            membership_authority,
            coven_protocol::store_commit::StoreCommitOperationsInput {
                stream_activations,
                circle_controls: vec![circle_reference],
                ..coven_protocol::store_commit::StoreCommitOperationsInput::empty()
            },
        )
        .map_err(|error| {
            crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                error.to_string(),
            )
        })
    }

    pub(crate) fn verify_prepared_circle_commit(
        &self,
        bytes: &[u8],
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        coord: coven_protocol::store_commit::StoreCommitCoord,
        object: coven_protocol::objects::ExactObjectRef,
    ) -> Result<
        coven_protocol::store_commit::VerifiedStoreBatchCommit,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        self.verify_prepared_commit(bytes, store_root_hash, coord, object)
            .map_err(|error| {
                crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                    error.to_string(),
                )
            })
    }

    pub(crate) fn sign_circle_store_head(
        &self,
        root_hash: coven_protocol::store_commit::ObjectHash,
        commit: coven_protocol::store_commit::StoreBatchCommitRef,
        history_summary: coven_protocol::store_commit::ObjectHash,
        successor: coven_protocol::store_commit::SuccessorLink,
    ) -> Result<
        coven_protocol::store_commit::StoreDeviceHead,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        self.sign_device_head(root_hash, commit, history_summary, successor)
            .map_err(|error| {
                crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                    error.to_string(),
                )
            })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_local_circle_snapshot_refs(
        &self,
        history: &mut crate::sync::store::owner::circles::VerifiedCircleHistory<'_, '_>,
        circle_id: coven_protocol::circle::CircleId,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
    ) -> Result<
        Vec<(
            coven_protocol::store_commit::CircleSnapshotRef,
            coven_protocol::store_commit::CircleSnapshotMeta,
        )>,
        crate::sync::store::owner::snapshot::SnapshotError,
    > {
        history
            .snapshots()
            .load_stream_refs(
                circle_id,
                access,
                self.registration.reference(),
                self.registration.value(),
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_local_circle_snapshots(
        &self,
        history: &mut crate::sync::store::owner::circles::VerifiedCircleHistory<'_, '_>,
        circle_id: coven_protocol::circle::CircleId,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
    ) -> Result<
        Vec<coven_protocol::store_commit::CircleSnapshotMeta>,
        crate::sync::store::owner::snapshot::SnapshotError,
    > {
        Ok(history
            .snapshots()
            .load_stream_refs(
                circle_id,
                access,
                self.registration.reference(),
                self.registration.value(),
            )
            .await?
            .into_iter()
            .map(|(_, snapshot)| snapshot)
            .collect())
    }
}
