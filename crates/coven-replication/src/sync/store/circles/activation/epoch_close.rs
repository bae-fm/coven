use super::*;

impl<'operation, 'storage> CircleActivationVerifier<'operation, 'storage> {
    pub(super) async fn verify_epoch_close(
        &self,
        commit: &StoreBatchCommit,
        control: &PreparedCircleControl,
        objects: &CircleActivationObjects,
        encryption: EncryptionService,
        roster_chain: &coven_protocol::circle::CircleRosterChain,
    ) -> Result<Option<VerifiedCloseOutcome>, CircleOperationError> {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            // The successor is an ActiveEpoch. Dispatch on the settled slot object,
            // never the epoch origin: a reopened founder-origin epoch keeps its
            // `Founder` origin, so origin-based dispatch would accept a forged reopen
            // that carries no cancellation.
            if objects.close_cancellation.is_some() {
                if objects.close_outcome.is_some() || objects.close_intent.is_some() {
                    return Err(CircleOperationError::InvalidState(
                        "Circle epoch reopen also carries a close outcome or intent".to_string(),
                    ));
                }
                self.verify_epoch_reopen(commit, control, objects).await?;
                return Ok(None);
            }
            if objects.close_intent.is_some() {
                return Err(CircleOperationError::InvalidState(
                    "active Circle control carries an epoch-close intent".to_string(),
                ));
            }
            return self
                .verify_epoch_close_outcome(commit, control, objects, encryption)
                .await;
        };
        if objects.close_outcome.is_some()
            || objects.close_cancellation.is_some()
            || close.frozen_device_state != commit.device_state
            || close.provisional_frontier
                != commit
                    .order
                    .predecessor_cut()
                    .map_err(CircleOperationError::from)?
                    .frontier()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch close differs from its activating Store cut".to_string(),
            ));
        }
        let intent = self
            .load_verified_epoch_close_intent(control, objects, encryption)
            .await?;
        let remaining = roster_chain
            .resolved_with_successor(intent.removal.clone())
            .map_err(CircleOperationError::from)?;
        if remaining.state_hash() != intent.remaining_roster_state_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch-close intent names another remaining roster".to_string(),
            ));
        }
        let remaining_members = remaining.members();
        let devices = self
            .database
            .resolved_store_device_state(&close.frozen_device_state)
            .await?;
        let mut expected = Vec::new();
        for record in devices.devices.values() {
            if !matches!(
                record.status,
                coven_protocol::store_commit::StoreDeviceStatus::Active
            ) {
                continue;
            }
            let registration = self
                .database
                .activated_store_device_registration(record.registration.clone())
                .await?;
            if remaining_members.contains_key(&registration.value().author_pubkey) {
                expected.push(record.registration.clone());
            }
        }
        expected.sort_by_key(|registration| registration.device_id);
        if close
            .participants
            .iter()
            .map(|participant| participant.registration.clone())
            .collect::<Vec<_>>()
            != expected
        {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch-close participants differ from the frozen active devices".to_string(),
            ));
        }
        Ok(None)
    }

    pub(super) async fn load_verified_epoch_close_intent(
        &self,
        control: &PreparedCircleControl,
        objects: &CircleActivationObjects,
        encryption: EncryptionService,
    ) -> Result<coven_protocol::circle::CircleEpochCloseIntent, CircleOperationError> {
        let store_root_hash = self.root().store_root_hash;
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch-close intent has no close control".to_string(),
            ));
        };
        if objects.close_intent.as_ref() != Some(&close.intent) {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch-close intent is absent from its exact object graph".to_string(),
            ));
        }
        let intent_prefix = circle_epoch_close_intent_semantic_prefix(
            control.value.circle_id,
            close.close_id,
            close.intent.intent_hash,
        );
        let bytes = self
            .storage
            .read_protocol_object(
                &ProtocolObjectContext::circle(
                    store_root_hash,
                    ProtocolObjectDomain::CircleEpochCloseIntent,
                    encryption,
                ),
                &close.intent.object,
                &intent_prefix,
            )
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let intent: coven_protocol::circle::CircleEpochCloseIntent =
            serde_json::from_slice(&bytes)?;
        if !intent.verify()
            || intent.store_root_hash != store_root_hash
            || intent.circle_id != control.value.circle_id
            || intent.close_id != close.close_id
            || intent.epoch_id != close.frozen_epoch.common.epoch_id
            || intent.predecessor_roster != close.frozen_epoch.roster
            || intent.owner_pubkey != control.value.author_pubkey
            || intent.intent_hash() != close.intent.intent_hash
        {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch-close intent failed exact verification".to_string(),
            ));
        }
        Ok(intent)
    }

    pub(super) async fn verify_epoch_close_outcome(
        &self,
        commit: &StoreBatchCommit,
        control: &PreparedCircleControl,
        objects: &CircleActivationObjects,
        encryption: EncryptionService,
    ) -> Result<Option<VerifiedCloseOutcome>, CircleOperationError> {
        let active = control.value.active_epoch().ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle epoch-close outcome has no active successor".to_string(),
            )
        })?;
        // An epoch's origin describes how the epoch was born, not what each control in
        // it does. Only the control whose exact predecessor is the `EpochClose`
        // finalizes the close and carries the outcome; every later control in the same
        // epoch (a re-add, a further add) inherits the already-settled epoch. Dispatch
        // on the retained predecessor's kind, not on the origin alone.
        let epoch_predecessor = match control.value.previous_control_hash() {
            None => None,
            Some(_) => {
                let coord = Self::reopen_predecessor_coord(control)?;
                self.database
                    .verified_circle_activation(self.root().clone(), control.value.circle_id, coord)
                    .await?
            }
        };
        let predecessor_is_close = epoch_predecessor.as_ref().is_some_and(|predecessor| {
            matches!(
                predecessor.control.value.state(),
                CircleControlState::EpochClose(_)
            )
        });
        let coven_protocol::circle::CircleEpochOrigin::Closed {
            closed_epoch_id,
            close_control,
            close_id,
            outcome_hash,
            cutoff,
        } = &active.common.origin
        else {
            if objects.close_outcome.is_some() {
                return Err(CircleOperationError::InvalidState(
                    "founder Circle epoch carries a close outcome".to_string(),
                ));
            }
            // A founder-origin active epoch is an ordinary transition only when its
            // predecessor is itself active. An active successor of an epoch close must
            // carry a settlement — a finalize outcome (origin `Closed`, handled above)
            // or a reopen cancellation (dispatched before this function). Reaching here
            // over an `EpochClose` predecessor is the forged reopen the cancellation
            // dispatch key closes: reject it rather than trusting the `Founder` origin.
            if predecessor_is_close {
                return Err(CircleOperationError::InvalidState(
                    "Circle active successor of an epoch close carries no settlement".to_string(),
                ));
            }
            return Ok(None);
        };
        if !predecessor_is_close {
            // A closed-origin control whose exact predecessor is already an active
            // epoch operates within an epoch a prior finalize established — the re-add
            // the plan defines as "the same operation with a new active access leaf and
            // current bootstrap." The outcome was verified once, at that finalize; this
            // control neither carries nor re-proves it. Bind it to the retained
            // predecessor's exact epoch so a forged origin (a fabricated cutoff or
            // close reference) cannot ride in on an in-epoch add.
            if objects.close_outcome.is_some() {
                return Err(CircleOperationError::InvalidState(
                    "in-epoch Circle control carries a close outcome".to_string(),
                ));
            }
            let epoch_predecessor = epoch_predecessor.ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "closed-origin in-epoch Circle control retained no exact predecessor"
                        .to_string(),
                )
            })?;
            if control.value.epoch_id() != epoch_predecessor.control.value.epoch_id()
                || control.value.key_fingerprint()
                    != epoch_predecessor.control.value.key_fingerprint()
                || active.common.origin != epoch_predecessor.control.value.active_common().origin
            {
                return Err(CircleOperationError::InvalidState(
                    "closed-origin in-epoch Circle control differs from its retained epoch"
                        .to_string(),
                ));
            }
            return Ok(None);
        }
        let outcome_ref = objects.close_outcome.as_ref().ok_or_else(|| {
            CircleOperationError::InvalidState(
                "closed Circle epoch omits its exact outcome".to_string(),
            )
        })?;
        if outcome_ref.close_id != *close_id || outcome_ref.outcome_hash != *outcome_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch origin differs from its outcome reference".to_string(),
            ));
        }
        let predecessor = self
            .database
            .verified_circle_activation(
                self.root().clone(),
                control.value.circle_id,
                close_control.clone(),
            )
            .await?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle successor retained no exact close activation".to_string(),
                )
            })?;
        let CircleControlState::EpochClose(close) = predecessor.control.value.state() else {
            return Err(CircleOperationError::InvalidState(
                "Circle successor origin names an active predecessor".to_string(),
            ));
        };
        if predecessor.control.coord != *close_control
            || close.close_id != *close_id
            || close.frozen_epoch.common.epoch_id != *closed_epoch_id
            || outcome_ref.object.slot() != &close.outcome_slot
            || !control.value.causally_covers(&predecessor.control.value)
        {
            return Err(CircleOperationError::InvalidState(
                "Circle successor differs from its exact epoch close".to_string(),
            ));
        }
        let intent = self
            .load_verified_epoch_close_intent(
                &predecessor.control,
                predecessor.reference.objects(),
                encryption,
            )
            .await?;
        let outcome_prefix = coven_protocol::circle::circle_epoch_close_outcome_semantic_prefix(
            control.value.circle_id,
            *close_id,
        );
        let outcome_bytes = self
            .storage
            .read_protocol_object(
                &ProtocolObjectContext::store_encrypted(
                    commit.store_root_hash,
                    ProtocolObjectDomain::CircleEpochCloseOutcome,
                ),
                &outcome_ref.object,
                &outcome_prefix,
            )
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let coven_protocol::circle::CircleEpochCloseSlotValue::Outcome(outcome) =
            coven_protocol::circle::CircleEpochCloseSlotValue::parse(&outcome_bytes)?
        else {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch-close outcome slot holds a cancellation for a finalized successor"
                    .to_string(),
            ));
        };
        if outcome.outcome_hash() != *outcome_hash
            || coven_protocol::circle::CircleEpochCloseOutcomeRef::from_outcome(
                &outcome,
                outcome_ref.object.clone(),
            )? != *outcome_ref
            || outcome.responses.len() != close.participants.len()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch-close outcome differs from its exact reference".to_string(),
            ));
        }
        let mut settlements = Vec::with_capacity(close.participants.len());
        for (participant, settlement_ref) in close.participants.iter().zip(&outcome.responses) {
            if settlement_ref.registration() != &participant.registration
                || settlement_ref.object().slot() != &participant.response_slot
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle epoch-close outcome settlement differs from its participant"
                        .to_string(),
                ));
            }
            let prefix = coven_protocol::circle::circle_epoch_close_response_semantic_prefix(
                control.value.circle_id,
                *close_id,
                participant.registration.device_id,
            );
            let bytes = self
                .storage
                .read_protocol_object(
                    &ProtocolObjectContext::store_encrypted(
                        commit.store_root_hash,
                        ProtocolObjectDomain::CircleEpochCloseResponse,
                    ),
                    settlement_ref.object(),
                    &prefix,
                )
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
            // Dispatch on the slot's actual settled arm, not the outcome's claim: the
            // outcome's declared settlement must equal the one derived here, which is
            // what refuses an outcome naming an exclusion for a slot holding a response.
            let slot_value =
                coven_protocol::circle::CircleEpochCloseResponseSlotValue::parse(&bytes)?;
            let settlement = match &slot_value {
                coven_protocol::circle::CircleEpochCloseResponseSlotValue::Response(response) => {
                    let registration = self
                        .database
                        .activated_store_device_registration(participant.registration.clone())
                        .await?;
                    if !response.verify_for(&predecessor.control, registration.value()) {
                        return Err(CircleOperationError::InvalidState(
                            "Circle epoch-close response failed exact verification".to_string(),
                        ));
                    }
                    coven_protocol::circle::CircleEpochCloseSettlement::Response(
                        coven_protocol::circle::CircleEpochCloseResponseRef::from_response(
                            response,
                            settlement_ref.object().clone(),
                        )?,
                    )
                }
                coven_protocol::circle::CircleEpochCloseResponseSlotValue::Exclusion(exclusion) => {
                    if !exclusion.verify_for(&predecessor.control)
                        || exclusion.excluded != participant.registration
                    {
                        return Err(CircleOperationError::InvalidState(
                            "Circle epoch-close exclusion failed exact verification".to_string(),
                        ));
                    }
                    coven_protocol::circle::CircleEpochCloseSettlement::Exclusion(
                        coven_protocol::circle::CircleEpochCloseExclusionRef::from_exclusion(
                            exclusion,
                            settlement_ref.object().clone(),
                        )?,
                    )
                }
            };
            settlements.push((settlement, slot_value));
        }
        let successor = coven_protocol::circle::CircleEpochSuccessor {
            epoch_id: active.common.epoch_id,
            key_fingerprint: active.common.key_fingerprint,
            owners: active.common.owners.clone(),
            access_root: active.common.access_root,
            metadata: active.metadata.clone(),
            roster: active.roster.clone(),
            store_membership: active.store_membership.clone(),
        };
        if !outcome.verify_for(&predecessor.control, &intent, &settlements)
            || outcome.cutoff != *cutoff
            || outcome.successor != successor
        {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch-close outcome failed exact verification".to_string(),
            ));
        }
        let exclusions = settlements
            .into_iter()
            .filter_map(|(settlement, _)| match settlement {
                coven_protocol::circle::CircleEpochCloseSettlement::Exclusion(reference) => {
                    Some(reference.registration)
                }
                coven_protocol::circle::CircleEpochCloseSettlement::Response(_) => None,
            })
            .collect();
        Ok(Some(VerifiedCloseOutcome {
            close_id: outcome.close_id,
            exclusions,
        }))
    }

    /// Load the reopening control's exact predecessor close coordinate. The reopen's
    /// same-stream predecessor is named by `previous_control_hash` and carried in the
    /// covered control heads.
    pub(super) fn reopen_predecessor_coord(
        control: &PreparedCircleControl,
    ) -> Result<coven_protocol::circle::CircleControlCoord, CircleOperationError> {
        let active = control.value.active_epoch().ok_or_else(|| {
            CircleOperationError::InvalidState("Circle reopen has no active successor".to_string())
        })?;
        let previous = control.value.previous_control_hash().ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle active successor of an epoch close names no predecessor".to_string(),
            )
        })?;
        active
            .covered_control_heads
            .iter()
            .find(|head| head.coord.control_hash == previous)
            .map(|head| head.coord.clone())
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle successor predecessor is absent from its covered control heads"
                        .to_string(),
                )
            })
    }

    /// Verify an `EpochClose → ActiveEpoch` reopen. The reopen is valid only when the
    /// exact-read outcome slot of the named predecessor close holds an Owner-signed
    /// cancellation, and the successor restores the frozen epoch's protocol identity
    /// exactly — re-issuing only the control-bound access material.
    pub(super) async fn verify_epoch_reopen(
        &self,
        commit: &StoreBatchCommit,
        control: &PreparedCircleControl,
        objects: &CircleActivationObjects,
    ) -> Result<(), CircleOperationError> {
        let active = control.value.active_epoch().ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle epoch reopen has no active successor".to_string(),
            )
        })?;
        let cancellation_ref = objects.close_cancellation.as_ref().ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle epoch reopen omits its exact cancellation".to_string(),
            )
        })?;
        let close_coord = Self::reopen_predecessor_coord(control)?;
        let predecessor = self
            .database
            .verified_circle_activation(
                self.root().clone(),
                control.value.circle_id,
                close_coord.clone(),
            )
            .await?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle reopen retained no exact close activation".to_string(),
                )
            })?;
        let CircleControlState::EpochClose(close) = predecessor.control.value.state() else {
            return Err(CircleOperationError::InvalidState(
                "Circle reopen predecessor is an active epoch, not a close".to_string(),
            ));
        };
        if predecessor.control.coord != close_coord
            || cancellation_ref.close_id != close.close_id
            || cancellation_ref.object.slot() != &close.outcome_slot
            || !control.value.causally_covers(&predecessor.control.value)
        {
            return Err(CircleOperationError::InvalidState(
                "Circle reopen differs from its exact epoch close".to_string(),
            ));
        }
        let cancellation_prefix =
            coven_protocol::circle::circle_epoch_close_outcome_semantic_prefix(
                control.value.circle_id,
                close.close_id,
            );
        let cancellation_bytes = self
            .storage
            .read_protocol_object(
                &ProtocolObjectContext::store_encrypted(
                    commit.store_root_hash,
                    ProtocolObjectDomain::CircleEpochCloseOutcome,
                ),
                &cancellation_ref.object,
                &cancellation_prefix,
            )
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let coven_protocol::circle::CircleEpochCloseSlotValue::Cancellation(cancellation) =
            coven_protocol::circle::CircleEpochCloseSlotValue::parse(&cancellation_bytes)?
        else {
            return Err(CircleOperationError::InvalidState(
                "Circle reopen outcome slot holds a final outcome, not a cancellation".to_string(),
            ));
        };
        if !cancellation.verify_for(&predecessor.control)
            || coven_protocol::circle::CircleEpochCloseCancellationRef::from_cancellation(
                &cancellation,
                cancellation_ref.object.clone(),
            )? != *cancellation_ref
        {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch-close cancellation failed exact verification".to_string(),
            ));
        }
        let frozen = &close.frozen_epoch;
        if active.common.epoch_id != frozen.common.epoch_id
            || active.common.key_fingerprint != frozen.common.key_fingerprint
            || active.common.owners != frozen.common.owners
            || active.common.origin != frozen.common.origin
            || active.metadata != frozen.metadata
            || active.roster != frozen.roster
        {
            return Err(CircleOperationError::InvalidState(
                "Circle reopen successor differs from its frozen epoch".to_string(),
            ));
        }
        Ok(())
    }
}
