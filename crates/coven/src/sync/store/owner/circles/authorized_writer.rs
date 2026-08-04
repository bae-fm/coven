use super::commands::{
    CircleAddMemberRequest, CircleCancelEpochCloseRequest, CircleDeleteRequest,
    CircleOperationRequest, CircleRemoveMemberRequest, CircleRenameRequest,
    CircleResolveControlRequest, CircleResolveLosingBranch,
};
use super::*;
use crate::keys;
use crate::protocol::circle::{
    CircleControlState, CircleEpochCloseResponseSlotValue, CircleId, CircleRole,
};
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};

pub(crate) struct AuthorizedCircleWriter<'writer, 'storage> {
    writer: &'writer mut AuthorizedWriterOperation<'storage>,
    database: crate::database::StoreDatabase,
    storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
    root: crate::protocol::store_commit::StoreRootRef,
    membership: crate::protocol::membership::MembershipChain,
    identity: &'storage crate::keys::UserKeypair,
    registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    registration: crate::protocol::store_commit::StoreDeviceRegistration,
    device_signer: crate::keys::UserKeypair,
}

impl<'writer, 'storage> AuthorizedCircleWriter<'writer, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        writer: &'writer mut AuthorizedWriterOperation<'storage>,
        database: crate::database::StoreDatabase,
        storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
        root: crate::protocol::store_commit::StoreRootRef,
        membership: crate::protocol::membership::MembershipChain,
        identity: &'storage crate::keys::UserKeypair,
        registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: crate::protocol::store_commit::StoreDeviceRegistration,
        device_signer: crate::keys::UserKeypair,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            root,
            membership,
            identity,
            registration_ref,
            registration,
            device_signer,
        }
    }

    pub(super) fn publisher(&mut self) -> publication::CircleCandidatePublisher<'_, 'storage> {
        let database = self.database.clone();
        let storage = self.storage.clone();
        let membership = self.membership.clone();
        let identity = self.identity;
        let history = self.writer.circle_history();
        publication::CircleCandidatePublisher::new(database, storage, membership, identity, history)
    }

    pub(super) fn preparer(&mut self) -> preparation::CircleCandidatePreparer<'_, 'storage> {
        let announcement_stream_id = self.writer.announcement_stream_id();
        let database = self.database.clone();
        let membership = self.membership.clone();
        let root = self.root.clone();
        let storage = self.storage.clone();
        let registration_ref = self.registration_ref.clone();
        let registration = self.registration.clone();
        let device_signer = self.device_signer.clone();
        let identity = self.identity;
        let history = self.writer.circle_history();
        preparation::CircleCandidatePreparer::new(
            announcement_stream_id,
            database,
            membership,
            root,
            storage,
            registration_ref,
            registration,
            device_signer,
            identity,
            history,
        )
    }

    #[cfg(test)]
    pub(crate) async fn prepare_create_for_test(
        &mut self,
        metadata_stamp: &str,
        name: &str,
    ) -> Result<CircleOperationJournal, CircleOperationError> {
        self.preparer().prepare_create(metadata_stamp, name).await
    }

    #[cfg(test)]
    pub(crate) async fn publish_prepared_operation_for_test(
        &mut self,
        operation_id: &crate::protocol::circle::CircleOperationId,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
    ) -> Result<(), CircleOperationError> {
        self.publisher().publish(operation_id, routing_key).await
    }

    #[cfg(test)]
    pub(crate) async fn prepare_circle_object_for_test(
        &mut self,
        context: &crate::storage::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
        bytes: Vec<u8>,
    ) -> Result<crate::storage::PreparedExactObject, CircleOperationError> {
        self.preparer()
            .prepare_circle_object(context, semantic_prefix, extension, bytes)
            .await
    }

    #[cfg(test)]
    pub(crate) fn prepare_circle_object_at_for_test(
        &mut self,
        context: &crate::storage::ProtocolObjectContext,
        slot: crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
        bytes: Vec<u8>,
    ) -> Result<crate::storage::PreparedExactObject, CircleOperationError> {
        self.preparer()
            .prepare_circle_object_at(context, slot, semantic_prefix, bytes)
    }

    #[cfg(test)]
    pub(crate) async fn resign_merge_journal_with_reference_for_test(
        &mut self,
        journal: &mut CircleOperationJournal,
        reference: crate::protocol::store_commit::CircleControlRef,
        mutate_commit: impl FnOnce(&mut crate::protocol::store_commit::StoreBatchCommit),
    ) -> Result<(), CircleOperationError> {
        let old_commit = journal.commit()?;
        let author = self
            .database
            .activated_store_device_registration(old_commit.author_registration.clone())
            .await?;
        let device_signer = author
            .value()
            .device_signer(self.identity)
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "derive Circle commit device signer: {error}"
                ))
            })?;
        let coord = journal.operation().commit_ref.coord.clone();
        let stream_activations = old_commit.stream_activations().to_vec();
        let mut commit = super::preparation::signed_circle_commit(
            old_commit.store_root_hash,
            old_commit.write_id.clone(),
            coord.clone(),
            old_commit.author_registration.clone(),
            author.value(),
            old_commit.order.clone(),
            old_commit.membership_state.clone(),
            old_commit.device_state.clone(),
            old_commit
                .operations_membership_authority()
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "prepared Circle commit has no validated operations authority: {error}"
                    ))
                })?,
            reference,
            stream_activations,
            &device_signer,
        )?;
        mutate_commit(&mut commit);
        commit.signature =
            crate::keys::sign_hex(&device_signer, &commit.canonical_signed_bytes()).1;
        let crate::protocol::store_commit::StoreCommitCoord { stream_id, .. } = coord.clone();
        let commit_prepared = self
            .preparer()
            .prepare_circle_object(
                &ProtocolObjectContext::signed_plaintext(
                    commit.store_root_hash,
                    ProtocolObjectDomain::StoreCommit,
                ),
                &crate::protocol::store_commit::commit_semantic_prefix(
                    commit.candidate_family(),
                    &stream_id.to_string(),
                    commit.seq(),
                    commit.commit_hash(),
                ),
                ".json",
                commit.to_bytes(),
            )
            .await?;
        let commit_ref = crate::protocol::store_commit::StoreBatchCommitRef::from_commit(
            &commit,
            coord,
            commit_prepared.reference().clone(),
        )
        .map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "build replacement Circle commit reference: {error}"
            ))
        })?;
        let old_head = journal.operation().policy.head.clone();
        let history_summary = journal.operation().policy.history_summary.clone();
        let head = crate::protocol::store_commit::StoreDeviceHead::signed(
            commit.store_root_hash,
            commit.author_registration.clone(),
            commit_ref.clone(),
            history_summary.digest(),
            old_head.successor,
            &device_signer,
        )
        .map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "sign replacement Circle Store head: {error}"
            ))
        })?;
        let head_slot = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle operation has no prepared Store head".to_string(),
                )
            })?
            .reference()
            .slot()
            .clone();
        let head_prepared = self.preparer().prepare_circle_object_at(
            &ProtocolObjectContext::signed_plaintext(
                commit.store_root_hash,
                ProtocolObjectDomain::StoreHead,
            ),
            head_slot,
            &crate::protocol::store_commit::head_slot_prefix(
                &commit.author_registration.device_id.to_string(),
                commit.seq(),
            ),
            head.to_bytes(),
        )?;
        let operation = journal.operation_mut();
        operation.commit_bytes = commit.to_bytes();
        operation.commit_ref = commit_ref;
        operation
            .prepared_objects
            .insert("store-commit".to_string(), commit_prepared);
        operation
            .prepared_objects
            .insert("store-head".to_string(), head_prepared);
        operation.policy = CircleOperationPolicy {
            head,
            history_summary,
        };
        operation.uploaded.clear();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn prepare_circle_activation_objects_for_test(
        &mut self,
        draft: crate::protocol::circle::CircleTransitionDraft,
        history: &CircleTransitionHistory,
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
    ) -> Result<
        (
            crate::protocol::circle::PreparedCircleTransition,
            crate::protocol::store_commit::CircleActivationObjects,
            std::collections::BTreeMap<String, crate::storage::PreparedExactObject>,
            Option<crate::storage::ExactObjectRef>,
            Vec<crate::protocol::store_commit::StreamActivation>,
        ),
        CircleOperationError,
    > {
        self.preparer()
            .prepare_circle_activation_objects(draft, history, &[], candidate_family)
            .await
    }

    pub(crate) fn close(&mut self) -> close_responses::CircleCloseCoordinator<'_, 'storage> {
        close_responses::CircleCloseCoordinator::new(
            self.writer,
            self.database.clone(),
            self.storage.clone(),
            self.root.clone(),
            self.membership.clone(),
            self.identity,
            self.registration_ref.clone(),
            self.registration.clone(),
            self.device_signer.clone(),
        )
    }

    pub(crate) fn acknowledgements(&mut self) -> acknowledgements::CircleAcknowledgementWriter {
        acknowledgements::CircleAcknowledgementWriter::new(
            self.database.clone(),
            self.storage.clone(),
            self.root.clone(),
            self.registration_ref.clone(),
            self.registration.clone(),
            self.device_signer.clone(),
        )
    }

    pub(crate) fn snapshots(&mut self) -> snapshots::CircleSnapshotWriter<'_, 'storage> {
        snapshots::CircleSnapshotWriter::new(
            self.writer,
            self.database.clone(),
            self.storage.clone(),
            self.root.clone(),
            self.registration_ref.clone(),
            self.registration.clone(),
            self.device_signer.clone(),
        )
    }

    fn history(&mut self) -> VerifiedCircleHistory<'_, 'storage> {
        self.writer.circle_history()
    }

    /// A deleted Circle is terminal: every lifecycle command refuses it with a
    /// typed reason rather than a generic missing-authoring-state error.
    async fn ensure_not_deleted(&self, circle_id: CircleId) -> Result<(), CircleOperationError> {
        if self.database.circle_is_deleted(circle_id).await? {
            return Err(CircleOperationError::Deleted { circle_id });
        }
        Ok(())
    }

    async fn current_authoring_context(
        &mut self,
        circle_id: CircleId,
    ) -> Result<
        (
            CircleAuthoringState,
            crate::protocol::store_commit::VerifiedStoreBatchCommit,
            crate::protocol::store_commit::CircleControlRef,
        ),
        CircleOperationError,
    > {
        self.ensure_not_deleted(circle_id).await?;
        let identity_pubkey = keys::public_key_hex(self.identity);
        let (current, activation_commit_ref) = self
            .database
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await?;
        let activation_commit = self
            .history()
            .load_commit(&activation_commit_ref)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        if activation_commit.value().candidate_family() != current.candidate_family {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle {circle_id} current state differs from its activating Store commit"
            )));
        }
        let reference = activation_commit
            .value()
            .circle_controls()
            .iter()
            .find(|reference| {
                reference.circle_id() == circle_id && reference.control() == &current.control.coord
            })
            .cloned()
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        Ok((current, activation_commit, reference))
    }

    async fn current_delete_context(
        &mut self,
        circle_id: CircleId,
    ) -> Result<
        (
            CircleAuthoringState,
            crate::protocol::store_commit::CircleControlRef,
        ),
        CircleOperationError,
    > {
        let identity_pubkey = keys::public_key_hex(self.identity);
        let (current, activation_commit_ref) = self
            .database
            .circle_delete_context(circle_id, &identity_pubkey)
            .await?;
        let activation_commit = self
            .history()
            .load_commit(&activation_commit_ref)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        if activation_commit.value().candidate_family() != current.candidate_family {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle {circle_id} current state differs from its activating Store commit"
            )));
        }
        let reference = activation_commit
            .value()
            .circle_controls()
            .iter()
            .find(|reference| {
                reference.circle_id() == circle_id && reference.control() == &current.control.coord
            })
            .cloned()
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        Ok((current, reference))
    }

    pub(crate) async fn create_circle(
        &mut self,
        metadata_stamp: &str,
        name: &str,
    ) -> Result<CircleId, CircleOperationError> {
        let journal = self.preparer().prepare_create(metadata_stamp, name).await?;
        let circle_id = journal.circle_id();
        let operation_id = journal.operation_id.clone();
        self.database.insert_circle_operation(journal).await?;
        self.publisher().publish(&operation_id, None).await?;
        Ok(circle_id)
    }

    pub(crate) async fn rename_circle(
        &mut self,
        metadata_stamp: &str,
        circle_id: CircleId,
        name: &str,
    ) -> Result<(), CircleOperationError> {
        let (current, _, reference) = self.current_authoring_context(circle_id).await?;
        let journal = self
            .preparer()
            .prepare_request(CircleOperationRequest::Rename(Box::new(
                CircleRenameRequest {
                    circle_id,
                    name: name.to_string(),
                    metadata_stamp: metadata_stamp.to_string(),
                    current,
                    previous_control: reference,
                },
            )))
            .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle rename changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        self.database.insert_circle_operation(journal).await?;
        self.publisher().publish(&operation_id, None).await
    }

    pub(crate) async fn remove_circle_member(
        &mut self,
        circle_id: CircleId,
        member_pubkey: String,
    ) -> Result<crate::protocol::circle::CircleOperationId, CircleOperationError> {
        let (current, activation_commit, reference) =
            self.current_authoring_context(circle_id).await?;
        let keyring = match &current.access.disposition {
            crate::protocol::circle::CircleAccessDisposition::Active { keyring, .. } => keyring,
            crate::protocol::circle::CircleAccessDisposition::Inactive => {
                return Err(CircleOperationError::InvalidState(
                    "Circle member removal requires active local access".to_string(),
                ));
            }
        };
        let roster_chain = self
            .history()
            .activations()
            .load_control_roster_chain(&activation_commit, &reference, &current.control, keyring)
            .await?;
        let journal = self
            .preparer()
            .prepare_request(CircleOperationRequest::RemoveMember(Box::new(
                CircleRemoveMemberRequest {
                    circle_id,
                    member_pubkey,
                    current,
                    previous_control: reference,
                    roster_chain,
                },
            )))
            .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle member removal changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        self.database.insert_circle_operation(journal).await?;
        self.publisher().publish(&operation_id, None).await?;
        Ok(operation_id)
    }

    pub(crate) async fn resolve_circle_control(
        &mut self,
        circle_id: CircleId,
        chosen: crate::protocol::circle::CircleControlCoord,
    ) -> Result<(), CircleOperationError> {
        let branches = self
            .database
            .circle_control_conflict_branches(circle_id)
            .await?
            .ok_or(CircleOperationError::NotConflicted { circle_id })?;
        let request = self
            .resolution_request(circle_id, &chosen, &branches, branches.clone())
            .await?;
        let journal = self.preparer().prepare_request(request).await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle control resolution changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        self.database.insert_circle_operation(journal).await?;
        self.publisher().publish(&operation_id, None).await
    }

    async fn resolution_request(
        &self,
        circle_id: CircleId,
        chosen: &crate::protocol::circle::CircleControlCoord,
        retained_branches: &[crate::protocol::circle::CircleControlCoord],
        conflicting_branches: Vec<crate::protocol::circle::CircleControlCoord>,
    ) -> Result<CircleOperationRequest, CircleOperationError> {
        self.ensure_not_deleted(circle_id).await?;
        if !retained_branches.contains(chosen) {
            return Err(CircleOperationError::ChosenBranchNotRetained { circle_id });
        }
        let identity_pubkey = keys::public_key_hex(self.identity);
        let chosen_activation = self
            .database
            .verified_circle_activation(self.root.clone(), circle_id, chosen.clone())
            .await?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} conflict omits retained authority for the chosen branch"
                ))
            })?;
        if matches!(
            chosen_activation.control.value.state(),
            CircleControlState::EpochClose(_)
        ) {
            return Err(CircleOperationError::ResolveToClosingBranch { circle_id });
        }
        let chosen_state =
            retained_branch_authoring_state(circle_id, &identity_pubkey, &chosen_activation)?;
        let previous_control = chosen_activation.reference.clone();
        let mut losing_branches = Vec::new();
        for branch in retained_branches {
            if branch == chosen {
                continue;
            }
            let activation = self
                .database
                .verified_circle_activation(self.root.clone(), circle_id, branch.clone())
                .await?
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle {circle_id} conflict omits retained authority for a losing branch"
                    ))
                })?;
            let selected_metadata = losing_branch_selected_metadata(circle_id, &activation)?;
            losing_branches.push(CircleResolveLosingBranch {
                reference: activation.reference.clone(),
                selected_metadata,
            });
        }
        Ok(CircleOperationRequest::ResolveControl(Box::new(
            CircleResolveControlRequest {
                circle_id,
                chosen: chosen_state,
                previous_control,
                losing_branches,
                conflicting_branches,
            },
        )))
    }

    #[cfg(test)]
    pub(super) async fn resolution_request_for_test(
        &self,
        circle_id: CircleId,
        chosen: &crate::protocol::circle::CircleControlCoord,
        conflicting_branches: Vec<crate::protocol::circle::CircleControlCoord>,
    ) -> Result<CircleOperationRequest, CircleOperationError> {
        let retained_branches = self
            .database
            .circle_control_conflict_branches(circle_id)
            .await?
            .ok_or(CircleOperationError::NotConflicted { circle_id })?;
        self.resolution_request(circle_id, chosen, &retained_branches, conflicting_branches)
            .await
    }

    pub(crate) async fn cancel_circle_epoch_close(
        &mut self,
        circle_id: CircleId,
    ) -> Result<crate::protocol::circle::CircleOperationId, CircleOperationError> {
        let operation_id = self
            .begin_circle_epoch_close_cancellation(circle_id)
            .await?;
        self.publisher().publish(&operation_id, None).await?;
        Ok(operation_id)
    }

    async fn begin_circle_epoch_close_cancellation(
        &mut self,
        circle_id: CircleId,
    ) -> Result<crate::protocol::circle::CircleOperationId, CircleOperationError> {
        self.ensure_not_deleted(circle_id).await?;
        let identity_pubkey = keys::public_key_hex(self.identity);
        let mut journal = self
            .database
            .waiting_circle_operations()
            .await?
            .into_iter()
            .find(|journal| journal.circle_id() == circle_id)
            .ok_or(CircleOperationError::NoCloseToCancel { circle_id })?;
        let member_pubkey = match &journal.intent {
            CircleOperationIntent::RemoveMember { member_pubkey } => member_pubkey.clone(),
            _ => {
                return Err(CircleOperationError::Journal(format!(
                    "Circle operation {} waits for close responses without a removal intent",
                    journal.operation_id
                )));
            }
        };
        let close_id =
            crate::protocol::circle::CircleEpochCloseId::from_operation_id(&journal.operation_id);
        let (current, reference) = match self
            .database
            .circle_control_conflict_branches(circle_id)
            .await?
        {
            None => {
                let (current, _) = self
                    .database
                    .circle_closing_context(circle_id, &identity_pubkey)
                    .await?;
                let CircleControlState::EpochClose(close) = current.control.value.state() else {
                    return Err(CircleOperationError::InvalidState(
                        "Circle close-cancellation context is active".to_string(),
                    ));
                };
                if close.close_id != close_id {
                    return Err(CircleOperationError::Journal(format!(
                        "Circle operation {} differs from its close id",
                        journal.operation_id
                    )));
                }
                let activation = self
                    .database
                    .verified_circle_activation(
                        self.root.clone(),
                        circle_id,
                        current.control.coord.clone(),
                    )
                    .await?
                    .ok_or_else(|| {
                        CircleOperationError::InvalidState(format!(
                            "Circle {circle_id} closing control has no retained activation"
                        ))
                    })?;
                if activation.control != current.control {
                    return Err(CircleOperationError::InvalidState(format!(
                        "Circle {circle_id} closing state differs from its retained activation"
                    )));
                }
                (current, activation.reference)
            }
            Some(branches) => {
                let mut selected = None;
                for branch in branches {
                    let activation = self
                        .database
                        .verified_circle_activation(self.root.clone(), circle_id, branch)
                        .await?
                        .ok_or_else(|| {
                            CircleOperationError::InvalidState(format!(
                                "Circle {circle_id} conflict omits a retained branch activation"
                            ))
                        })?;
                    let CircleControlState::EpochClose(close) = activation.control.value.state()
                    else {
                        continue;
                    };
                    if close.close_id != close_id {
                        continue;
                    }
                    let current =
                        retained_branch_authoring_state(circle_id, &identity_pubkey, &activation)?;
                    if selected
                        .replace((current, activation.reference.clone()))
                        .is_some()
                    {
                        return Err(CircleOperationError::InvalidState(format!(
                            "Circle {circle_id} conflict repeats close {close_id}"
                        )));
                    }
                }
                selected.ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle {circle_id} conflict does not retain local close {close_id}"
                    ))
                })?
            }
        };
        let CircleControlState::EpochClose(close) = current.control.value.state() else {
            return Err(CircleOperationError::InvalidState(
                "Circle close-cancellation context is active".to_string(),
            ));
        };
        if close.close_id
            != crate::protocol::circle::CircleEpochCloseId::from_operation_id(&journal.operation_id)
        {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} differs from its close id",
                journal.operation_id
            )));
        }
        let prepared = self
            .preparer()
            .prepare_request(CircleOperationRequest::CancelEpochClose(Box::new(
                CircleCancelEpochCloseRequest {
                    operation_id: journal.operation_id.clone(),
                    circle_id,
                    member_pubkey,
                    current,
                    previous_control: reference,
                },
            )))
            .await?;
        if prepared.operation_id != journal.operation_id
            || prepared.circle_id != circle_id
            || prepared.intent != journal.intent
        {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} cancellation changed its durable identity",
                journal.operation_id
            )));
        }
        journal.begin_finalization(prepared.operation().clone())?;
        self.database
            .begin_circle_operation_finalization(journal.clone())
            .await?;
        Ok(journal.operation_id)
    }

    #[cfg(test)]
    pub(crate) async fn begin_circle_epoch_close_cancellation_for_test(
        &mut self,
        circle_id: CircleId,
    ) -> Result<crate::protocol::circle::CircleOperationId, CircleOperationError> {
        self.begin_circle_epoch_close_cancellation(circle_id).await
    }

    pub(crate) async fn exclude_circle_close_device(
        &mut self,
        circle_id: CircleId,
        excluded_device_id: crate::protocol::store_commit::StoreDeviceId,
    ) -> Result<(), CircleOperationError> {
        let identity_pubkey = keys::public_key_hex(self.identity);
        let journal = self
            .database
            .waiting_circle_operations()
            .await?
            .into_iter()
            .find(|journal| journal.circle_id() == circle_id)
            .ok_or(CircleOperationError::NoCloseToExclude { circle_id })?;
        let (current, _) = self
            .database
            .circle_closing_context(circle_id, &identity_pubkey)
            .await?;
        let CircleControlState::EpochClose(close) = current.control.value.state() else {
            return Err(CircleOperationError::InvalidState(
                "Circle device-exclusion context is active".to_string(),
            ));
        };
        if close.close_id
            != crate::protocol::circle::CircleEpochCloseId::from_operation_id(&journal.operation_id)
        {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} differs from its close id",
                journal.operation_id
            )));
        }
        let participant = close
            .participants
            .iter()
            .find(|participant| participant.registration.device_id == excluded_device_id)
            .ok_or(CircleOperationError::DeviceNotACloseParticipant {
                circle_id,
                device_id: excluded_device_id,
            })?;
        let exclusion = crate::protocol::circle::CircleEpochCloseExclusion::signed(
            &current.control,
            participant.registration.clone(),
            self.identity,
        )?;
        let prefix = crate::protocol::circle::circle_epoch_close_response_semantic_prefix(
            circle_id,
            close.close_id,
            excluded_device_id,
        );
        let context = ProtocolObjectContext::store_encrypted(
            current.control.value.store_root_hash,
            ProtocolObjectDomain::CircleEpochCloseResponse,
        );
        let prepared = self
            .storage
            .prepare_protocol_object(
                &context,
                participant.response_slot.clone(),
                &prefix,
                CircleEpochCloseResponseSlotValue::Exclusion(exclusion).to_bytes(),
            )
            .map_err(crate::storage::StoreObjectError::from)?;
        match self.storage.create_protocol_object(&prepared).await {
            Ok(()) | Err(StorageError::SlotCollision(_)) => {}
            Err(error) => return Err(crate::storage::StoreObjectError::from(error).into()),
        }
        let (winner_bytes, _) = self
            .storage
            .read_prepared_protocol_slot(&context, &participant.response_slot, &prefix)
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
        match CircleEpochCloseResponseSlotValue::parse(&winner_bytes)? {
            CircleEpochCloseResponseSlotValue::Exclusion(winner) => {
                if !winner.verify_for(&current.control) {
                    return Err(CircleOperationError::InvalidState(
                        "published Circle epoch-close exclusion failed verification".to_string(),
                    ));
                }
            }
            CircleEpochCloseResponseSlotValue::Response(response) => {
                let registration = self
                    .database
                    .activated_store_device_registration(participant.registration.clone())
                    .await?;
                if !response.verify_for(&current.control, registration.value()) {
                    return Err(CircleOperationError::InvalidState(
                        "Circle epoch-close response slot holds an unverifiable response"
                            .to_string(),
                    ));
                }
                tracing::debug!(
                    circle_id = %circle_id,
                    close_id = %close.close_id,
                    device_id = %excluded_device_id,
                    "device responded before exclusion; adopting its response"
                );
            }
        }
        Ok(())
    }

    pub(crate) async fn retry_circle_operation(
        &mut self,
        operation_id: &crate::protocol::circle::CircleOperationId,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<(), CircleOperationError> {
        let journal = self
            .database
            .circle_operation(operation_id)
            .await?
            .ok_or_else(|| {
                CircleOperationError::Journal(format!("circle operation {operation_id} is absent"))
            })?;
        if !matches!(
            journal.state(),
            crate::protocol::circle::CircleOperationState::Blocked { .. }
        ) {
            return Err(CircleOperationError::NotBlocked {
                operation_id: operation_id.clone(),
            });
        }
        self.database.unblock_circle_operation(operation_id).await?;
        let routing_key = routing_encryption
            .map(|encryption| {
                crate::protocol::circle::derive_row_routing_key(
                    encryption,
                    journal.operation().creation.control.value.store_root_hash,
                )
            })
            .transpose()
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        self.publisher()
            .publish(operation_id, routing_key.as_ref())
            .await
    }

    pub(crate) async fn delete_circle(
        &mut self,
        circle_id: CircleId,
    ) -> Result<(), CircleOperationError> {
        if self
            .database
            .circle_control_conflict_branches(circle_id)
            .await?
            .is_some()
        {
            return Err(CircleOperationError::Conflicted { circle_id });
        }
        if self.database.circle_is_deleted(circle_id).await? {
            return Err(CircleOperationError::Deleted { circle_id });
        }
        let (current, reference) = self.current_delete_context(circle_id).await?;
        let journal = self
            .preparer()
            .prepare_request(CircleOperationRequest::Delete(Box::new(
                CircleDeleteRequest {
                    circle_id,
                    current,
                    previous_control: reference,
                },
            )))
            .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle deletion changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        let superseded = self
            .database
            .waiting_circle_operations()
            .await?
            .into_iter()
            .find(|waiting| waiting.circle_id() == circle_id)
            .map(|waiting| waiting.operation_id.clone());
        match superseded {
            Some(superseded) => {
                self.database
                    .insert_circle_operation_superseding(journal, superseded)
                    .await?
            }
            None => self.database.insert_circle_operation(journal).await?,
        }
        self.publisher().publish(&operation_id, None).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn add_circle_member(
        &mut self,
        circle_id: CircleId,
        member_pubkey: String,
        role: CircleRole,
        bootstrap: crate::sync::store::SnapshotCut,
        routing_key: &crate::protocol::circle::RowRoutingKey,
    ) -> Result<(), CircleOperationError> {
        let (current, activation_commit, reference) =
            self.current_authoring_context(circle_id).await?;
        let keyring = match &current.access.disposition {
            crate::protocol::circle::CircleAccessDisposition::Active { keyring, .. } => keyring,
            crate::protocol::circle::CircleAccessDisposition::Inactive => {
                return Err(CircleOperationError::InvalidState(
                    "Circle member addition requires active local access".to_string(),
                ));
            }
        };
        let roster_chain = self
            .history()
            .activations()
            .load_control_roster_chain(&activation_commit, &reference, &current.control, keyring)
            .await?;
        let journal = self
            .preparer()
            .prepare_request(CircleOperationRequest::AddMember(Box::new(
                CircleAddMemberRequest {
                    circle_id,
                    member_pubkey,
                    role,
                    bootstrap,
                    current,
                    previous_control: reference,
                    roster_chain,
                },
            )))
            .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle member addition changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        self.database.insert_circle_operation(journal).await?;
        self.publisher()
            .publish(&operation_id, Some(routing_key))
            .await
    }

    pub(crate) async fn resume_circle_operations(
        &mut self,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
    ) -> Result<(), CircleOperationError> {
        let database = self.database.clone();
        for operation_id in database.discarding_circle_operations().await? {
            self.history()
                .cleanup_operation_candidate(&operation_id)
                .await
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "Circle operation {operation_id} discard cleanup: {error}"
                    ))
                })?;
            database
                .finish_circle_operation_discard(&operation_id)
                .await?;
        }
        while let Some(journal) = database.oldest_pending_circle_operation().await? {
            if !journal.is_publishable() {
                return Err(CircleOperationError::Journal(format!(
                    "pending circle operation {} contains a blocked payload",
                    journal.circle_id()
                )));
            }
            match self
                .publisher()
                .publish(&journal.operation_id, routing_key)
                .await
            {
                Ok(()) | Err(CircleOperationError::Blocked { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

fn retained_branch_authoring_state(
    circle_id: CircleId,
    identity_pubkey: &str,
    activation: &crate::sync::store::circle_controls::VerifiedCircleReference,
) -> Result<CircleAuthoringState, CircleOperationError> {
    let access = activation.local_access.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(format!(
            "Circle {circle_id} retained branch has no local access"
        ))
    })?;
    let active = access.active.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(format!(
            "Circle {circle_id} retained branch has no active access"
        ))
    })?;
    if access.leaf.value.recipient_pubkey != identity_pubkey {
        return Err(CircleOperationError::InvalidState(format!(
            "Circle {circle_id} retained branch belongs to another local identity"
        )));
    }
    Ok(CircleAuthoringState {
        candidate_family: access.leaf.value.candidate_family,
        control: activation.control.clone(),
        access: access.leaf.value.clone(),
        roster: active.roster.clone(),
        metadata: active.metadata.clone(),
    })
}

fn losing_branch_selected_metadata(
    circle_id: CircleId,
    activation: &crate::sync::store::circle_controls::VerifiedCircleReference,
) -> Result<crate::protocol::circle::CircleMetadata, CircleOperationError> {
    activation
        .local_access
        .as_ref()
        .and_then(|access| access.active.as_ref())
        .map(|active| active.metadata.clone())
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle {circle_id} control resolution requires active access to every branch"
            ))
        })
}
