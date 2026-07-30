use super::{
    prepare_circle_operation, prepare_circle_operation_request, CircleAuthoringState,
    CircleOperationError, CircleOperationIntent, CircleTransitionHistory,
};
use crate::database::StoreDatabase;
use crate::keys;
use crate::protocol::circle::{
    circle_epoch_close_response_semantic_prefix, CircleCloseParticipant, CircleCloseSettlement,
    CircleCloseStatus, CircleControlState, CircleEpochCloseResponseSlotValue, CircleId, CircleRole,
    CircleRosterChain,
};
use crate::protocol::store_commit::CircleControlRef;
use crate::storage::BlobPathScheme;
use crate::storage::StoreObjectError;
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage};
use crate::sync::store::{AuthorizedWriterOperation, Store};

/// A deleted Circle is terminal: every lifecycle command refuses it with a
/// typed reason rather than a generic missing-authoring-state error.
async fn ensure_not_deleted(
    database: &StoreDatabase,
    circle_id: CircleId,
) -> Result<(), CircleOperationError> {
    if database.circle_is_deleted(circle_id).await? {
        return Err(CircleOperationError::Deleted { circle_id });
    }
    Ok(())
}

impl Store {
    /// The read-only settlement status of a Circle's in-flight epoch close: for
    /// each participant device, whether its create-once response slot holds a
    /// response, an Owner exclusion, or is still empty. Reports each slot's
    /// declared settlement; the finalize path verifies each slot before acting on
    /// it. A read, so it does not require Owner authorization — any participant
    /// resolving the closing control can inspect it.
    pub(crate) async fn circle_close_status(
        &self,
        circle_id: CircleId,
    ) -> Result<CircleCloseStatus, CircleOperationError> {
        let identity_pubkey = keys::public_key_hex(self.identity());
        let (current, _) = self
            .database()
            .circle_closing_context(circle_id, &identity_pubkey)
            .await?;
        let CircleControlState::EpochClose(close) = current.control.value.state() else {
            return Err(CircleOperationError::InvalidState(
                "Circle close-status inspection received an active control".to_string(),
            ));
        };
        let context = ProtocolObjectContext::store_encrypted(
            current.control.value.store_root_hash,
            ProtocolObjectDomain::CircleEpochCloseResponse,
        );
        let storage = self.storage();
        let mut participants = Vec::with_capacity(close.participants.len());
        for participant in &close.participants {
            let prefix = circle_epoch_close_response_semantic_prefix(
                current.control.value.circle_id,
                close.close_id,
                participant.registration.device_id,
            );
            let settlement = match storage
                .read_protocol_slot(&context, &participant.response_slot, &prefix)
                .await
            {
                Ok((bytes, _)) => match CircleEpochCloseResponseSlotValue::parse(&bytes).map_err(
                    |error| {
                        CircleOperationError::InvalidState(format!(
                            "Circle epoch-close response slot for device {} failed to parse: {error}",
                            participant.registration.device_id
                        ))
                    },
                )? {
                    CircleEpochCloseResponseSlotValue::Response(_) => {
                        CircleCloseSettlement::Responded
                    }
                    CircleEpochCloseResponseSlotValue::Exclusion(_) => {
                        CircleCloseSettlement::Excluded
                    }
                },
                Err(StorageError::NotFound(_)) => CircleCloseSettlement::Pending,
                Err(error) => return Err(StoreObjectError::from(error).into()),
            };
            participants.push(CircleCloseParticipant {
                device_id: participant.registration.device_id,
                settlement,
            });
        }
        Ok(CircleCloseStatus {
            circle_id,
            close_id: close.close_id,
            participants,
        })
    }

    pub(crate) async fn create_circle(
        &self,
        metadata_stamp: &str,
        name: &str,
    ) -> Result<CircleId, CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let database = authority.database().clone();
        let journal = Box::pin(prepare_circle_operation(
            &mut authority,
            metadata_stamp,
            name,
        ))
        .await?;
        let circle_id = journal.circle_id();
        let operation_id = journal.operation_id.clone();
        database.insert_circle_operation(journal).await?;
        authority
            .circle_operation()
            .publish(&operation_id, None)
            .await?;
        Ok(circle_id)
    }

    pub(crate) async fn rename_circle(
        &self,
        metadata_stamp: &str,
        circle_id: CircleId,
        name: &str,
    ) -> Result<(), CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let database = self.database();
        ensure_not_deleted(database, circle_id).await?;
        let identity_pubkey = keys::public_key_hex(self.identity());
        let (current, activation_commit_ref) = database
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await?;
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let database = authority.database().clone();
        let activation_commit = authority
            .history_verifier_mut()
            .load_ref(&activation_commit_ref)
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
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        let journal = Box::pin(prepare_circle_operation_request(
            &mut authority,
            CircleOperationRequest::Rename(Box::new(CircleRenameRequest {
                circle_id,
                name: name.to_string(),
                metadata_stamp: metadata_stamp.to_string(),
                current,
                previous_control: reference.clone(),
            })),
        ))
        .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle rename changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        database.insert_circle_operation(journal).await?;
        authority
            .circle_operation()
            .publish(&operation_id, None)
            .await
    }

    pub(crate) async fn remove_circle_member(
        &self,
        circle_id: CircleId,
        member_pubkey: String,
    ) -> Result<crate::protocol::circle::CircleOperationId, CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let database = self.database();
        ensure_not_deleted(database, circle_id).await?;
        let identity_pubkey = keys::public_key_hex(self.identity());
        let (current, activation_commit_ref) = database
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await?;
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let database = authority.database().clone();
        let activation_commit = authority
            .history_verifier_mut()
            .load_ref(&activation_commit_ref)
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
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        let keyring = match &current.access.disposition {
            crate::protocol::circle::CircleAccessDisposition::Active { keyring, .. } => keyring,
            crate::protocol::circle::CircleAccessDisposition::Inactive => {
                return Err(CircleOperationError::InvalidState(
                    "Circle member removal requires active local access".to_string(),
                ));
            }
        };
        let roster_chain = super::activation::load_circle_control_roster_chain(
            &database,
            authority.history_verifier_mut(),
            &activation_commit,
            reference,
            &current.control,
            keyring,
        )
        .await?;
        let journal = Box::pin(prepare_circle_operation_request(
            &mut authority,
            CircleOperationRequest::RemoveMember(Box::new(CircleRemoveMemberRequest {
                circle_id,
                member_pubkey,
                current,
                previous_control: reference.clone(),
                roster_chain,
            })),
        ))
        .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle member removal changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        database.insert_circle_operation(journal).await?;
        authority
            .circle_operation()
            .publish(&operation_id, None)
            .await?;
        Ok(operation_id)
    }

    /// Resolve a Circle whose control history forked into concurrent valid
    /// successors by authoring a covering successor of the chosen branch. This
    /// is callable on a conflicted Circle regardless of rotation state — it is
    /// deliberately allowed during required rotation, because resolution is the
    /// exit path out of the conflict and a conflicted Circle
    /// has no single resolved roster to evaluate rotation against. A
    /// rotation-required Circle re-derives that state from the resolved
    /// successor and blocks new content afterward.
    pub(crate) async fn resolve_circle_control(
        &self,
        circle_id: CircleId,
        chosen: crate::protocol::circle::CircleControlCoord,
    ) -> Result<(), CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let database = self.database();
        ensure_not_deleted(database, circle_id).await?;
        let branches = database
            .circle_control_conflict_branches(circle_id)
            .await?
            .ok_or(CircleOperationError::NotConflicted { circle_id })?;
        if !branches.contains(&chosen) {
            return Err(CircleOperationError::ChosenBranchNotRetained { circle_id });
        }
        let identity_pubkey = keys::public_key_hex(self.identity());
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let root = authority.store_root().clone();
        let database = authority.database().clone();
        let chosen_activation = database
            .verified_circle_activation(root.clone(), circle_id, chosen.clone())
            .await?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} conflict omits retained authority for the chosen branch"
                ))
            })?;
        // Resolving to a closing branch would author a new control coordinate
        // carrying the close. Participant responses bind to the closing control's
        // coordinate at create-once slots, so a resolution successor strands any
        // response already made against the original closing control, with no way
        // to re-respond. Refuse it: the Owner resolves to a non-closing branch to
        // discard the close, or lets the close settle before resolving.
        if matches!(
            chosen_activation.control.value.state(),
            crate::protocol::circle::CircleControlState::EpochClose(_)
        ) {
            return Err(CircleOperationError::ResolveToClosingBranch { circle_id });
        }
        let chosen_state =
            retained_branch_authoring_state(circle_id, &identity_pubkey, &chosen_activation)?;
        let previous_control = chosen_activation.reference.clone();
        let mut losing_branches = Vec::new();
        for branch in &branches {
            if *branch == chosen {
                continue;
            }
            let activation = database
                .verified_circle_activation(root.clone(), circle_id, branch.clone())
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
        let journal = Box::pin(prepare_circle_operation_request(
            &mut authority,
            CircleOperationRequest::ResolveControl(Box::new(CircleResolveControlRequest {
                circle_id,
                chosen: chosen_state,
                previous_control,
                losing_branches,
                conflicting_branches: branches,
            })),
        ))
        .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle control resolution changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        database.insert_circle_operation(journal).await?;
        authority
            .circle_operation()
            .publish(&operation_id, None)
            .await
    }

    /// Cancel the local device's in-flight epoch close by settling its one outcome
    /// slot with an Owner-signed cancellation and activating a reopening control
    /// that restores the frozen epoch. When concurrent controls have made the
    /// Circle conflicted, the durable operation's close id selects its exact
    /// retained branch; reopening that branch leaves the other branches visible
    /// for an explicit control resolution. Refuses if no local close operation is
    /// waiting for responses — a close whose outcome already won the slot has
    /// moved out of the waiting state and cannot be cancelled.
    pub(crate) async fn cancel_circle_epoch_close(
        &self,
        circle_id: CircleId,
    ) -> Result<crate::protocol::circle::CircleOperationId, CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let database = self.database();
        ensure_not_deleted(database, circle_id).await?;
        let identity_pubkey = keys::public_key_hex(self.identity());
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let root = authority.store_root().clone();
        let database = authority.database().clone();
        let mut journal = database
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
        let (current, reference) = local_close_cancellation_context(
            &database,
            &root,
            circle_id,
            &identity_pubkey,
            &journal,
        )
        .await?;
        let crate::protocol::circle::CircleControlState::EpochClose(close) =
            current.control.value.state()
        else {
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
        let prepared = Box::pin(prepare_circle_operation_request(
            &mut authority,
            CircleOperationRequest::CancelEpochClose(Box::new(CircleCancelEpochCloseRequest {
                operation_id: journal.operation_id.clone(),
                circle_id,
                member_pubkey,
                current,
                previous_control: reference,
            })),
        ))
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
        database
            .begin_circle_operation_finalization(journal.clone())
            .await?;
        authority
            .circle_operation()
            .publish(&journal.operation_id, None)
            .await?;
        Ok(journal.operation_id)
    }

    /// Sign and publish an Owner exclusion of an unavailable participant device to
    /// that device's create-once close-response slot, letting a stalled close reach
    /// completion. Create-once decides the slot: if the device's own response
    /// landed first, the exclusion is a no-op and that response is adopted.
    pub(crate) async fn exclude_circle_close_device(
        &self,
        circle_id: CircleId,
        excluded_device_id: crate::protocol::store_commit::StoreDeviceId,
    ) -> Result<(), CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let database = authority.database().clone();
        let storage = authority.history_verifier_mut().storage();
        let identity_pubkey = keys::public_key_hex(self.identity());
        let journal = database
            .waiting_circle_operations()
            .await?
            .into_iter()
            .find(|journal| journal.circle_id() == circle_id)
            .ok_or(CircleOperationError::NoCloseToExclude { circle_id })?;
        let (current, _) = database
            .circle_closing_context(circle_id, &identity_pubkey)
            .await?;
        let crate::protocol::circle::CircleControlState::EpochClose(close) =
            current.control.value.state()
        else {
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
            self.identity(),
        )?;
        let prefix = crate::protocol::circle::circle_epoch_close_response_semantic_prefix(
            circle_id,
            close.close_id,
            excluded_device_id,
        );
        let context = crate::storage::ProtocolObjectContext::store_encrypted(
            current.control.value.store_root_hash,
            crate::storage::ProtocolObjectDomain::CircleEpochCloseResponse,
        );
        let prepared = storage
            .prepare_protocol_object(
                &context,
                participant.response_slot.clone(),
                &prefix,
                crate::protocol::circle::CircleEpochCloseResponseSlotValue::Exclusion(exclusion)
                    .to_bytes(),
            )
            .map_err(crate::storage::StoreObjectError::from)?;
        match storage.create_protocol_object(&prepared).await {
            Ok(()) | Err(crate::storage::StorageError::SlotCollision(_)) => {}
            Err(error) => return Err(crate::storage::StoreObjectError::from(error).into()),
        }
        let (winner_bytes, _) = storage
            .read_prepared_protocol_slot(&context, &participant.response_slot, &prefix)
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
        match crate::protocol::circle::CircleEpochCloseResponseSlotValue::parse(&winner_bytes)? {
            crate::protocol::circle::CircleEpochCloseResponseSlotValue::Exclusion(winner) => {
                if !winner.verify_for(&current.control) {
                    return Err(CircleOperationError::InvalidState(
                        "published Circle epoch-close exclusion failed verification".to_string(),
                    ));
                }
            }
            crate::protocol::circle::CircleEpochCloseResponseSlotValue::Response(response) => {
                let registration = database
                    .activated_store_device_registration(participant.registration.clone())
                    .await?;
                if !response.verify_for(&current.control, &registration) {
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

    /// Return a blocked operation to its captured phase and re-enter the publish
    /// pipeline, which revalidates against refreshed signed state. Initiator-driven
    /// — the cycle never auto-unblocks. Refuses typed if the operation is not
    /// blocked; retrying twice converges because publication is per-step
    /// idempotent and re-blocks if authority is still absent.
    pub(crate) async fn retry_circle_operation(
        &self,
        operation_id: &crate::protocol::circle::CircleOperationId,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<(), CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let database = authority.database().clone();
        let journal = database
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
        database.unblock_circle_operation(operation_id).await?;
        let routing_key = routing_encryption
            .map(|encryption| {
                crate::protocol::circle::derive_row_routing_key(
                    encryption,
                    journal.operation().creation.control.value.store_root_hash,
                )
            })
            .transpose()
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        authority
            .circle_operation()
            .publish(operation_id, routing_key.as_ref())
            .await
    }

    /// Discard a durable Circle operation that can provably never activate,
    /// exact-deleting its candidate-exclusive objects and clearing its journal
    /// row. Legal only with one of the three direct nonactivation proofs — a
    /// different verified winner already occupies the candidate's successor slot,
    /// the author was permanently excluded, or a membership revocation forecloses
    /// activation. Without proof it refuses typed: it never assumes an unseen
    /// candidate failed to activate. Idempotent and restart-safe — a crash between
    /// the recorded proof and the cleared row resumes the same cleanup from the
    /// durable `Discarding` state.
    pub(crate) async fn discard_circle_operation(
        &self,
        operation_id: &crate::protocol::circle::CircleOperationId,
    ) -> Result<(), CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let mut authorized = self
            .authorize()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let root = authorized.store_root().clone();
        let database = authorized.database().clone();
        let journal = database
            .circle_operation(operation_id)
            .await?
            .ok_or_else(|| {
                CircleOperationError::Journal(format!("circle operation {operation_id} is absent"))
            })?;
        if !journal.is_discarding() {
            let discard_candidate = database
                .circle_operation_discard_candidate(operation_id)
                .await?;
            let Some(nonactivation) = authorized
                .history()
                .discard_candidate_nonactivation(
                    &discard_candidate.candidate,
                    discard_candidate.revoked_grant.as_ref(),
                )
                .await?
            else {
                return Err(CircleOperationError::DiscardRequiresNonactivation {
                    operation_id: operation_id.clone(),
                });
            };
            database
                .begin_circle_operation_discard(root, operation_id, nonactivation)
                .await?;
        }
        authorized
            .cleanup_circle_operation_candidate(operation_id)
            .await
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "Circle operation {operation_id} discard cleanup: {error}"
                ))
            })?;
        database
            .finish_circle_operation_discard(operation_id)
            .await?;
        Ok(())
    }

    /// Author the terminal deletion of a Circle. It requires a resolved current
    /// state — a conflicted Circle is refused until the Owner resolves it,
    /// because the conflicting set may bury membership intent — and refuses a
    /// Circle that is already deleted. It is not gated by the rotation-required
    /// check: deletion distributes no key, so it is a terminal exit like member
    /// removal.
    pub(crate) async fn delete_circle(
        &self,
        circle_id: CircleId,
    ) -> Result<(), CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let database = authority.database().clone();
        if database
            .circle_control_conflict_branches(circle_id)
            .await?
            .is_some()
        {
            return Err(CircleOperationError::Conflicted { circle_id });
        }
        if database.circle_is_deleted(circle_id).await? {
            return Err(CircleOperationError::Deleted { circle_id });
        }
        let identity_pubkey = keys::public_key_hex(self.identity());
        let (current, activation_commit_ref) = database
            .circle_delete_context(circle_id, &identity_pubkey)
            .await?;
        let activation_commit = authority
            .history_verifier_mut()
            .load_ref(&activation_commit_ref)
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
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        let journal = Box::pin(prepare_circle_operation_request(
            &mut authority,
            CircleOperationRequest::Delete(Box::new(CircleDeleteRequest {
                circle_id,
                current,
                previous_control: reference.clone(),
            })),
        ))
        .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle deletion changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        // A closing Circle holds a waiting close operation in its single operation
        // slot. The terminal deletion supersedes that close: remove the waiting
        // operation and take the slot in one transaction. An active Circle has no
        // operation waiting, so the deletion inserts into a free slot.
        let superseded = database
            .waiting_circle_operations()
            .await?
            .into_iter()
            .find(|waiting| waiting.circle_id() == circle_id)
            .map(|waiting| waiting.operation_id.clone());
        match superseded {
            Some(superseded) => {
                database
                    .insert_circle_operation_superseding(journal, superseded)
                    .await?
            }
            None => database.insert_circle_operation(journal).await?,
        }
        authority
            .circle_operation()
            .publish(&operation_id, None)
            .await
    }
}

impl AuthorizedWriterOperation<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn add_circle_member(
        &mut self,
        circle_id: CircleId,
        member_pubkey: String,
        role: CircleRole,
        bootstrap: crate::sync::store::snapshot::SnapshotCut,
        routing_key: &crate::protocol::circle::RowRoutingKey,
    ) -> Result<(), CircleOperationError> {
        let database = self.database().clone();
        ensure_not_deleted(&database, circle_id).await?;
        let identity_pubkey = keys::public_key_hex(self.writer.identity);
        let (current, activation_commit_ref) = database
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await?;
        let activation_commit = self
            .history_verifier_mut()
            .load_ref(&activation_commit_ref)
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
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        let keyring = match &current.access.disposition {
            crate::protocol::circle::CircleAccessDisposition::Active { keyring, .. } => keyring,
            crate::protocol::circle::CircleAccessDisposition::Inactive => {
                return Err(CircleOperationError::InvalidState(
                    "Circle member addition requires active local access".to_string(),
                ));
            }
        };
        let roster_chain = super::activation::load_circle_control_roster_chain(
            &database,
            self.history_verifier_mut(),
            &activation_commit,
            reference,
            &current.control,
            keyring,
        )
        .await?;
        let journal = Box::pin(prepare_circle_operation_request(
            self,
            CircleOperationRequest::AddMember(Box::new(CircleAddMemberRequest {
                circle_id,
                member_pubkey,
                role,
                bootstrap,
                current,
                previous_control: reference.clone(),
                roster_chain,
            })),
        ))
        .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle member addition changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        database.insert_circle_operation(journal).await?;
        self.circle_operation()
            .publish(&operation_id, Some(routing_key))
            .await
    }

    pub(crate) async fn resume_circle_operations(
        &mut self,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
    ) -> Result<(), CircleOperationError> {
        let database = self.database().clone();
        // Interrupted discards resume first: a durable `Discarding` row plus the
        // per-object cleanup states carry an unfinished discard to completion
        // before any pending operation republishes.
        for operation_id in database.discarding_circle_operations().await? {
            self.cleanup_circle_operation_candidate(&operation_id)
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
                .circle_operation()
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

/// Build one retained branch's authoring inputs from its verified activation.
/// A conflicted Circle exposes no single `authoring_state`, so a command reads
/// the branch's roster, metadata, keyring, and access leaf from that activation.
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

/// Resolve the exact retained close branch named by the local waiting operation.
/// A resolved Circle reads its single closing state. A conflicted Circle dispatches
/// over the authoritative retained branch set and requires exactly one branch to
/// carry the operation-derived close id.
async fn local_close_cancellation_context(
    database: &StoreDatabase,
    root: &crate::protocol::store_commit::StoreRootRef,
    circle_id: CircleId,
    identity_pubkey: &str,
    journal: &super::CircleOperationJournal,
) -> Result<(CircleAuthoringState, CircleControlRef), CircleOperationError> {
    let close_id =
        crate::protocol::circle::CircleEpochCloseId::from_operation_id(&journal.operation_id);
    let Some(branches) = database.circle_control_conflict_branches(circle_id).await? else {
        let (current, _) = database
            .circle_closing_context(circle_id, identity_pubkey)
            .await?;
        let crate::protocol::circle::CircleControlState::EpochClose(close) =
            current.control.value.state()
        else {
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
        let activation = database
            .verified_circle_activation(root.clone(), circle_id, current.control.coord.clone())
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
        return Ok((current, activation.reference));
    };

    let mut selected = None;
    for branch in branches {
        let activation = database
            .verified_circle_activation(root.clone(), circle_id, branch)
            .await?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} conflict omits a retained branch activation"
                ))
            })?;
        let crate::protocol::circle::CircleControlState::EpochClose(close) =
            activation.control.value.state()
        else {
            continue;
        };
        if close.close_id != close_id {
            continue;
        }
        let current = retained_branch_authoring_state(circle_id, identity_pubkey, &activation)?;
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
    })
}

/// The metadata entry a losing conflict branch selected, read from its retained
/// active access. The resolution's name is the canonical maximum across every
/// branch's selection, so the resolver — an Owner holding the epoch key, and thus
/// active access to every branch — reads each losing branch's selected metadata
/// directly from its retained activation.
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

pub(super) struct CircleRenameRequest {
    pub(super) circle_id: CircleId,
    pub(super) name: String,
    pub(super) metadata_stamp: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
}

pub(super) struct CircleAddMemberRequest {
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) role: CircleRole,
    pub(super) bootstrap: crate::sync::store::snapshot::SnapshotCut,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    pub(super) roster_chain: CircleRosterChain,
}

pub(super) struct CircleRemoveMemberRequest {
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    pub(super) roster_chain: CircleRosterChain,
}

pub(super) struct CircleDeleteRequest {
    pub(super) circle_id: CircleId,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
}

pub(super) struct CircleResolveControlRequest {
    pub(super) circle_id: CircleId,
    pub(super) chosen: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    /// The retained branches other than `chosen`. The resolution merges each
    /// one's control, metadata, and roster head frontiers into its own so no
    /// author-stream head slot is re-allocated once the conflict collapses.
    pub(super) losing_branches: Vec<CircleResolveLosingBranch>,
    /// Every retained branch coordinate, in canonical order, as captured when
    /// the command ran. Preparation verifies this still equals the currently
    /// retained conflict set inside the journal transaction, so a branch
    /// discovered between command and activation resurfaces as a new conflict
    /// rather than being silently swallowed.
    pub(super) conflicting_branches: Vec<crate::protocol::circle::CircleControlCoord>,
}

pub(super) struct CircleResolveLosingBranch {
    /// The losing branch's exact activation reference: its control head plus the
    /// full activation objects (metadata and roster head frontiers and their
    /// entries) the resolution covers.
    pub(super) reference: CircleControlRef,
    /// The metadata entry this branch selected — one input to the resolution's
    /// deterministic name selection over the merged frontier.
    pub(super) selected_metadata: crate::protocol::circle::CircleMetadata,
}

pub(super) struct CircleFinalizeEpochCloseRequest {
    pub(super) operation_id: crate::protocol::circle::CircleOperationId,
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) metadata_stamp: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    pub(super) roster_chain: CircleRosterChain,
    pub(super) intent: crate::protocol::circle::CircleEpochCloseIntent,
    pub(super) responses: Vec<crate::protocol::circle::CircleEpochCloseSettlement>,
    pub(super) bootstrap: crate::sync::store::snapshot::SnapshotCut,
}

pub(super) struct CircleCancelEpochCloseRequest {
    pub(super) operation_id: crate::protocol::circle::CircleOperationId,
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
}

pub(super) enum CircleOperationRequest {
    Create {
        name: String,
        metadata_stamp: String,
    },
    Rename(Box<CircleRenameRequest>),
    AddMember(Box<CircleAddMemberRequest>),
    RemoveMember(Box<CircleRemoveMemberRequest>),
    ResolveControl(Box<CircleResolveControlRequest>),
    Delete(Box<CircleDeleteRequest>),
    FinalizeEpochClose(Box<CircleFinalizeEpochCloseRequest>),
    CancelEpochClose(Box<CircleCancelEpochCloseRequest>),
}

impl CircleOperationRequest {
    pub(super) fn intent(&self) -> CircleOperationIntent {
        match self {
            Self::Create { name, .. } => CircleOperationIntent::Create { name: name.clone() },
            Self::Rename(request) => CircleOperationIntent::Rename {
                name: request.name.clone(),
            },
            Self::AddMember(request) => CircleOperationIntent::AddMember {
                member_pubkey: request.member_pubkey.clone(),
                role: request.role,
            },
            Self::RemoveMember(request) => CircleOperationIntent::RemoveMember {
                member_pubkey: request.member_pubkey.clone(),
            },
            Self::ResolveControl(request) => CircleOperationIntent::ResolveControl {
                chosen: request.chosen.control.coord.clone(),
            },
            Self::Delete(_) => CircleOperationIntent::Delete,
            Self::FinalizeEpochClose(request) => CircleOperationIntent::RemoveMember {
                member_pubkey: request.member_pubkey.clone(),
            },
            Self::CancelEpochClose(request) => CircleOperationIntent::RemoveMember {
                member_pubkey: request.member_pubkey.clone(),
            },
        }
    }

    pub(super) fn history(&self) -> CircleTransitionHistory {
        match self {
            Self::Create { .. } => CircleTransitionHistory::Founder,
            Self::Rename(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
            Self::AddMember(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
            Self::RemoveMember(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
            Self::ResolveControl(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
            Self::Delete(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
            Self::FinalizeEpochClose(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
            Self::CancelEpochClose(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
        }
    }

    /// The stable operation id and derived write identity for a close settlement.
    /// Finalize and cancel settle the same durable operation but derive distinct
    /// write identities, so a crashed settlement resumes as the kind it began as
    /// rather than being re-derived into the other.
    pub(super) fn settlement(
        &self,
    ) -> Option<(crate::protocol::circle::CircleOperationId, crate::WriteId)> {
        match self {
            Self::FinalizeEpochClose(request) => Some((
                request.operation_id.clone(),
                request.operation_id.finalization_write_id(),
            )),
            Self::CancelEpochClose(request) => Some((
                request.operation_id.clone(),
                request.operation_id.cancellation_write_id(),
            )),
            Self::Create { .. }
            | Self::Rename(_)
            | Self::AddMember(_)
            | Self::RemoveMember(_)
            | Self::ResolveControl(_)
            | Self::Delete(_) => None,
        }
    }
}
