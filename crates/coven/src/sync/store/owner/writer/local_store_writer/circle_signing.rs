use super::*;

impl LocalStoreWriter {
    pub(crate) fn verify_circle_roster_head(
        &self,
        head: &crate::protocol::circle::CircleRosterHead,
    ) -> bool {
        head.verify_for_registration(self.registration.value())
    }

    pub(crate) fn verify_circle_metadata_head(
        &self,
        head: &crate::protocol::circle::CircleMetadataHead,
    ) -> bool {
        head.verify_for_registration(self.registration.value())
    }

    pub(crate) fn sign_circle_roster_head(
        &self,
        entry: &crate::protocol::circle::CircleRosterEntry,
        tip: crate::protocol::objects::ExactObjectRef,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> crate::protocol::circle::CircleRosterHead {
        crate::protocol::circle::CircleRosterHead::signed(
            entry,
            tip,
            successor,
            &self.device_signer,
        )
    }

    pub(crate) fn sign_circle_metadata_head(
        &self,
        metadata: &crate::protocol::circle::CircleMetadata,
        tip: crate::protocol::objects::ExactObjectRef,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> crate::protocol::circle::CircleMetadataHead {
        crate::protocol::circle::CircleMetadataHead::signed(
            metadata,
            tip,
            successor,
            &self.device_signer,
        )
    }

    pub(crate) fn sign_circle_control_head(
        &self,
        control: &crate::protocol::circle::CircleControl,
        entry: crate::protocol::objects::ExactObjectRef,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> crate::protocol::circle::CircleControlHead {
        crate::protocol::circle::CircleControlHead::signed(
            control,
            entry,
            successor,
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finalize_circle_epoch_close(
        &self,
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        metadata_stamp: &str,
        store_membership: crate::protocol::circle_control::StoreMembershipStateRef,
        membership_authority: crate::protocol::membership::MembershipGrantCreationAuthority,
        store_members: Vec<(String, crate::protocol::membership::MemberRole)>,
        close_control: &crate::protocol::circle::PreparedCircleControl,
        current_roster: &crate::protocol::circle::CircleMaterializedRoster,
        current_roster_chain: crate::protocol::circle::CircleRosterChain,
        current_metadata: &crate::protocol::circle::CircleMetadata,
        keyring: &str,
        intent: crate::protocol::circle::CircleEpochCloseIntent,
        responses: Vec<crate::protocol::circle::CircleEpochCloseSettlement>,
        ids: &dyn crate::id_provider::IdProvider,
    ) -> Result<
        crate::protocol::circle::CircleTransitionDraft,
        crate::protocol::circle::CircleTransitionError,
    > {
        crate::protocol::circle::CircleTransitionDraft::finalize_epoch_close(
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
        control: &crate::protocol::circle::PreparedCircleControl,
        excluded: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        crate::protocol::circle::CircleEpochCloseExclusion,
        crate::protocol::circle::CircleTransitionError,
    > {
        crate::protocol::circle::CircleEpochCloseExclusion::signed(
            control,
            excluded,
            &self.identity,
        )
    }

    pub(crate) async fn load_circle_activations(
        &self,
        history: &mut crate::sync::store::owner::circles::VerifiedCircleHistory<'_, '_>,
        verified: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
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
        circle_id: crate::protocol::circle::CircleId,
        sequence: u64,
    ) -> String {
        crate::protocol::store_commit::circle_ack_slot_prefix(
            circle_id,
            &self.registration.value().device_id.to_string(),
            sequence,
        )
    }

    pub(crate) fn circle_ack_first_slot(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<crate::protocol::objects::ObjectSlot, crate::protocol::objects::StorageError> {
        crate::protocol::objects::ObjectSlot::logical(format!(
            "{}.json",
            self.circle_ack_semantic_prefix(circle_id, 1)
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_circle_acknowledgement(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        circle_id: crate::protocol::circle::CircleId,
        sequence: u64,
        frontier: crate::protocol::store_commit::CommitFrontier,
        control: crate::protocol::circle::CircleControlCoord,
        epoch_id: crate::protocol::circle::CircleEpochId,
        key_fingerprint: crate::encryption::KeyFingerprint,
        seeded_from: Option<crate::protocol::circle::CircleBootstrapCoverageRef>,
        sync_time: String,
        predecessor: Option<crate::protocol::objects::ExactObjectRef>,
        next_slot: crate::protocol::objects::ObjectSlot,
    ) -> Result<
        crate::protocol::store_commit::CircleAck,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        let activation = crate::protocol::store_commit::StreamActivation::device_authorized(
            root_hash,
            self.registration.reference().clone(),
            crate::protocol::store_commit::DeviceStreamAnchor::CircleAcknowledgements {
                circle_id,
                first_slot: self.circle_ack_first_slot(circle_id).map_err(|error| {
                    crate::protocol::store_commit::StoreProtocolError::Malformed(error.to_string())
                })?,
            },
        )
        .activation_id();
        crate::protocol::store_commit::CircleAck::signed(
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
            crate::protocol::store_commit::SuccessorLink {
                activation,
                predecessor,
                next_slot,
            },
            &self.device_signer,
        )
    }

    pub(crate) fn local_circle_close_participant(
        &self,
        close: &crate::protocol::circle::CircleEpochClose,
    ) -> Option<crate::protocol::circle::CircleEpochCloseParticipant> {
        close
            .participants
            .iter()
            .find(|participant| participant.registration == *self.registration.reference())
            .cloned()
    }

    pub(crate) fn sign_circle_epoch_close_response(
        &self,
        control: &crate::protocol::circle::PreparedCircleControl,
        frontier: crate::protocol::store_commit::CommitFrontier,
    ) -> Result<
        crate::protocol::circle::CircleEpochCloseResponse,
        crate::protocol::circle::CircleTransitionError,
    > {
        crate::protocol::circle::CircleEpochCloseResponse::signed(
            control,
            self.registration.reference().clone(),
            frontier,
            self.registration.value(),
            &self.device_signer,
        )
    }

    pub(crate) fn verify_local_circle_epoch_close_response(
        &self,
        response: &crate::protocol::circle::CircleEpochCloseResponse,
        control: &crate::protocol::circle::PreparedCircleControl,
    ) -> bool {
        response.verify_for(control, self.registration.value())
    }

    pub(crate) fn circle_snapshot_semantic_prefix(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        generation: u64,
    ) -> String {
        crate::protocol::store_commit::circle_snapshot_slot_prefix(
            circle_id,
            &self.registration.value().device_id.to_string(),
            generation,
        )
    }

    pub(crate) fn circle_snapshot_image_semantic_prefix(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        image_hash: crate::protocol::store_commit::ObjectHash,
    ) -> String {
        crate::protocol::store_commit::circle_snapshot_image_semantic_prefix(
            circle_id,
            &self.registration.value().device_id.to_string(),
            image_hash,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_circle_snapshot_meta(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        circle_id: crate::protocol::circle::CircleId,
        control: crate::protocol::circle::CircleControlCoord,
        epoch_id: crate::protocol::circle::CircleEpochId,
        key_fingerprint: crate::KeyFingerprint,
        generation: u64,
        bootstrap: crate::protocol::circle::CircleBootstrapRef,
        created_at: String,
        predecessor: Option<crate::protocol::store_commit::CircleSnapshotRef>,
        next_slot: crate::protocol::objects::ObjectSlot,
    ) -> Result<
        crate::protocol::store_commit::CircleSnapshotMeta,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        let activation = crate::protocol::store_commit::circle_snapshot_stream_activation(
            root_hash,
            self.registration.reference(),
            circle_id,
            &self.registration.value().device_id.to_string(),
        )?;
        crate::protocol::store_commit::CircleSnapshotMeta::signed(
            root_hash,
            circle_id,
            self.registration.reference().clone(),
            control,
            epoch_id,
            key_fingerprint,
            generation,
            bootstrap,
            created_at,
            crate::protocol::store_commit::CircleSnapshotSuccessorLink {
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
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        write_id: crate::WriteId,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        order: crate::protocol::store_commit::StoreCommitOrder,
        membership_state: crate::protocol::circle_control::StoreMembershipStateRef,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        membership_authority: crate::protocol::store_commit::StoreOperationMembershipAuthority,
        circle_reference: crate::protocol::store_commit::CircleControlRef,
        stream_activations: Vec<crate::protocol::store_commit::StreamActivation>,
    ) -> Result<
        crate::protocol::store_commit::StoreBatchCommit,
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
            crate::protocol::store_commit::StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations,
                circle_controls: vec![circle_reference],
                store_package: None,
                circle_packages: &[],
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
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        object: crate::protocol::objects::ExactObjectRef,
    ) -> Result<
        crate::protocol::store_commit::VerifiedStoreBatchCommit,
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
        root_hash: crate::protocol::store_commit::ObjectHash,
        commit: crate::protocol::store_commit::StoreBatchCommitRef,
        history_summary: crate::protocol::store_commit::ObjectHash,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> Result<
        crate::protocol::store_commit::StoreDeviceHead,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        self.sign_device_head(root_hash, commit, history_summary, successor)
            .map_err(|error| {
                crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                    error.to_string(),
                )
            })
    }

    #[cfg(test)]
    pub(crate) async fn load_local_circle_snapshot_refs(
        &self,
        history: &mut crate::sync::store::owner::circles::VerifiedCircleHistory<'_, '_>,
        circle_id: crate::protocol::circle::CircleId,
        access: &crate::protocol::circle_activation::CircleEpochAccess,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::CircleSnapshotRef,
            crate::protocol::store_commit::CircleSnapshotMeta,
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
        circle_id: crate::protocol::circle::CircleId,
        access: &crate::protocol::circle_activation::CircleEpochAccess,
    ) -> Result<
        Vec<crate::protocol::store_commit::CircleSnapshotMeta>,
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
