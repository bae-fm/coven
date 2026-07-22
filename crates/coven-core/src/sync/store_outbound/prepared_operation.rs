use super::*;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedStoreOperationCommit {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) prepared: PreparedExactObject,
    pub(crate) reference: StoreBatchCommitRef,
    pub(super) publication: StoreOperationPublication,
    pub(crate) registration_activation: Option<DeviceJoinRegistrationActivation>,
}

impl PreparedStoreOperationCommit {
    fn candidate_remote_objects(
        &self,
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreOutboundError> {
        let mut objects = vec![super::remote_object::RemoteObjectRecord::candidate_commit(
            self.reference.clone(),
            self.commit.to_bytes(),
            self.prepared.stored_bytes().to_vec(),
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?];
        if let Some((head, prepared)) = self.merge_publication() {
            objects.push(
                super::remote_object::RemoteObjectRecord::candidate_activated_store_head(
                    super::store_commit::StoreDeviceHeadRef {
                        head_hash: head.head_hash(),
                        object: prepared.reference().clone(),
                    },
                    head.to_bytes(),
                    prepared.stored_bytes().to_vec(),
                    self.reference.clone(),
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
            );
        }
        Ok(objects)
    }

    pub(crate) fn merge_membership_activation_remote_objects(
        &self,
        transition: &super::invite::PreparedMembershipTransition,
        publication: &super::invite::PreparedMembershipPublication,
        wraps: &[super::wrapped_store_key::PreparedWrappedStoreKey],
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreOutboundError> {
        self.validate_closed_shape()
            .map_err(StoreOutboundError::InvalidOutbound)?;
        super::invite::validate_prepared_transition(transition)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        super::invite::validate_prepared_publication(publication)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        if self.reference.coord.policy() != crate::WritePolicy::MergeConcurrent
            || self.commit.control()
                != Some(&StoreControl::MergeMembership {
                    transition: transition.transition.clone(),
                })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                super::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &self.reference
            )
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "Merge membership authority graph differs from its activating Store candidate"
                    .to_string(),
            ));
        }
        let expected_wraps = match &transition.entry.change {
            super::membership::MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys,
            _ => {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Merge membership removal graph contains another change".to_string(),
                ))
            }
        };
        if expected_wraps.len() != wraps.len()
            || expected_wraps
                .iter()
                .zip(wraps)
                .any(|(reference, prepared)| reference != &prepared.reference)
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "Merge membership removal wraps differ from its exact entry".to_string(),
            ));
        }
        self.close_merge_membership_remote_objects(transition, publication, wraps, Vec::new())
    }

    pub(crate) fn merge_membership_resolution_remote_objects(
        &self,
        transition: &super::invite::PreparedMembershipTransition,
        publication: &super::invite::PreparedMembershipPublication,
        resolution: &super::membership::StoreMembershipConflictResolution,
        reference: &super::membership::StoreMembershipConflictResolutionRef,
        prepared: &PreparedExactObject,
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreOutboundError> {
        self.validate_closed_shape()
            .map_err(StoreOutboundError::InvalidOutbound)?;
        super::invite::validate_prepared_transition(transition)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        super::invite::validate_prepared_publication(publication)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        if self.reference.coord.policy() != crate::WritePolicy::MergeConcurrent
            || self.commit.control()
                != Some(&StoreControl::MergeMembership {
                    transition: transition.transition.clone(),
                })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                super::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &self.reference
            )
            || !matches!(
                &transition.entry.change,
                super::membership::MembershipChange::ResolutionActivation {
                    resolution: introduced,
                } if introduced == reference
            )
            || reference.object != *prepared.reference()
            || reference.resolution_hash != resolution.resolution_hash()
            || reference.conflict_hash != resolution.conflict_hash
            || reference.resolver_pubkey != resolution.resolver_pubkey
            || prepared.stored_bytes()
                != serde_json::to_vec(resolution).map_err(|error| {
                    StoreOutboundError::InvalidOutbound(format!(
                        "serialize Store membership resolution: {error}"
                    ))
                })?
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "Merge membership resolution graph differs from its activating Store candidate"
                    .to_string(),
            ));
        }
        let authority =
            super::remote_object::RemoteObjectRecord::candidate_activated_store_membership_resolution(
                reference.clone(),
                serde_json::to_vec(resolution).map_err(|error| {
                    StoreOutboundError::InvalidOutbound(format!(
                        "serialize Store membership resolution: {error}"
                    ))
                })?,
                prepared.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        self.close_merge_membership_remote_objects(transition, publication, &[], vec![authority])
    }

    pub(crate) fn merge_owner_promotion_remote_objects(
        &self,
        transition: &super::invite::PreparedMembershipTransition,
        publication: &super::invite::PreparedMembershipPublication,
        wrapped_key: &super::wrapped_store_key::PreparedWrappedStoreKey,
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreOutboundError> {
        self.validate_closed_shape()
            .map_err(StoreOutboundError::InvalidOutbound)?;
        super::invite::validate_prepared_transition(transition)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        super::invite::validate_prepared_publication(publication)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        if self.reference.coord.policy() != crate::WritePolicy::MergeConcurrent
            || self.commit.control()
                != Some(&StoreControl::MergeMembership {
                    transition: transition.transition.clone(),
                })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                super::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &self.reference
            )
            || !matches!(
                &transition.entry.change,
                super::membership::MembershipChange::SetMember { wrapped_key: expected, role: super::membership::StoreMembershipRoleGrant::Owner { .. }, .. }
                    if expected == &wrapped_key.reference
            )
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "Merge Owner-promotion graph differs from its activating Store candidate"
                    .to_string(),
            ));
        }
        self.close_merge_membership_remote_objects(
            transition,
            publication,
            std::slice::from_ref(wrapped_key),
            Vec::new(),
        )
    }

    fn close_merge_membership_remote_objects(
        &self,
        transition: &super::invite::PreparedMembershipTransition,
        publication: &super::invite::PreparedMembershipPublication,
        wraps: &[super::wrapped_store_key::PreparedWrappedStoreKey],
        authorities: Vec<super::remote_object::RemoteObjectRecord>,
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreOutboundError> {
        let family = self.commit.candidate_family();
        let mut objects = self.candidate_remote_objects()?;
        let entry_bytes = serde_json::to_vec(&transition.entry).map_err(|error| {
            StoreOutboundError::InvalidOutbound(format!(
                "serialize Merge membership candidate entry: {error}"
            ))
        })?;
        objects.push(
            super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_entry(
                family,
                transition.entry_ref.clone(),
                entry_bytes,
                transition.entry_object.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
        );
        objects.push(
            super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_head(
                family,
                publication.head_ref.clone(),
                serde_json::to_vec(&publication.head).map_err(|error| {
                    StoreOutboundError::InvalidOutbound(format!(
                        "serialize Merge membership candidate head: {error}"
                    ))
                })?,
                publication.head_object.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
        );
        for prepared in wraps {
            let value = prepared.validate().map_err(StoreObjectError::from)?;
            let canonical = serde_json::to_vec(&value).map_err(|error| {
                StoreOutboundError::InvalidOutbound(format!(
                    "serialize Merge membership candidate wrap: {error}"
                ))
            })?;
            objects.push(
                super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_wrapped_store_key(
                    family,
                    prepared.reference.clone(),
                    canonical,
                    prepared.object.stored_bytes().to_vec(),
                    self.reference.clone(),
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
            );
        }
        objects.extend(authorities);
        let mut unique = std::collections::BTreeSet::new();
        if objects
            .iter()
            .any(|object| !unique.insert(object.object_id()))
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "Merge membership authority graph repeats an exact object".to_string(),
            ));
        }
        Ok(objects)
    }

    pub(crate) fn validate_closed_shape(&self) -> Result<(), String> {
        self.reference
            .verify_commit(&self.commit)
            .map_err(|error| error.to_string())?;
        if self.prepared.reference() != &self.reference.object
            || self.prepared.stored_bytes() != self.commit.to_bytes()
        {
            return Err(
                "prepared Store operation does not bind its exact commit bytes".to_string(),
            );
        }
        match &self.publication {
            StoreOperationPublication::MergeConcurrent {
                head,
                prepared,
                history_summary,
            } => {
                if self.reference.coord.policy() != crate::WritePolicy::MergeConcurrent
                    || head.commit != self.reference
                    || prepared.stored_bytes() != head.to_bytes()
                    || head.history_summary != history_summary.digest()
                    || history_summary.causal_cut.get(&self.reference.coord)
                        != Some(&self.reference)
                    || history_summary.validate_shape().is_err()
                    || matches!(
                        self.commit.control(),
                        Some(StoreControl::MergeMembership { .. })
                    ) != history_summary
                        .membership_proofs
                        .contains_key(&self.reference)
                {
                    return Err(
                        "prepared Merge operation does not bind its exact activation head"
                            .to_string(),
                    );
                }
            }
            StoreOperationPublication::Serial { head, .. } => {
                if self.reference.coord.policy() != crate::WritePolicy::Serial
                    || !matches!(
                        &head.state,
                        StoreSerialHeadState::Commit { commit, .. }
                            if commit == &self.reference
                    )
                {
                    return Err(
                        "prepared Serial operation does not bind its exact coordination head"
                            .to_string(),
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) fn has_same_durable_activation_as(&self, other: &Self) -> bool {
        self.reference == other.reference
            && self.commit.to_bytes() == other.commit.to_bytes()
            && self.prepared.reference() == other.prepared.reference()
            && self.registration_activation == other.registration_activation
            && match (&self.publication, &other.publication) {
                (
                    StoreOperationPublication::MergeConcurrent {
                        head,
                        prepared,
                        history_summary,
                    },
                    StoreOperationPublication::MergeConcurrent {
                        head: other_head,
                        prepared: other_prepared,
                        history_summary: other_history_summary,
                    },
                ) => {
                    head.to_bytes() == other_head.to_bytes()
                        && prepared.reference() == other_prepared.reference()
                        && history_summary == other_history_summary
                }
                (
                    StoreOperationPublication::Serial {
                        base_head,
                        head,
                        authorization_after,
                    },
                    StoreOperationPublication::Serial {
                        base_head: other_base_head,
                        head: other_head,
                        authorization_after: other_authorization_after,
                    },
                ) => {
                    base_head == other_base_head
                        && head.to_bytes() == other_head.to_bytes()
                        && authorization_after == other_authorization_after
                }
                _ => false,
            }
    }

    pub(crate) fn merge_publication(&self) -> Option<(&StoreDeviceHead, &PreparedExactObject)> {
        match &self.publication {
            StoreOperationPublication::MergeConcurrent { head, prepared, .. } => {
                Some((head, prepared))
            }
            StoreOperationPublication::Serial { .. } => None,
        }
    }

    pub(crate) fn serial_authorization_after(&self) -> Option<&SerialAuthorizationState> {
        match &self.publication {
            StoreOperationPublication::Serial {
                authorization_after,
                ..
            } => Some(authorization_after),
            StoreOperationPublication::MergeConcurrent { .. } => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn merge_publication_for_test(
        &self,
    ) -> Option<(&StoreDeviceHead, &PreparedExactObject)> {
        self.merge_publication()
    }

    #[cfg(test)]
    pub(crate) fn serial_publication_for_test(
        &self,
    ) -> Option<(&VersionedObject, &StoreSerialHead)> {
        match &self.publication {
            StoreOperationPublication::MergeConcurrent { .. } => None,
            StoreOperationPublication::Serial {
                base_head, head, ..
            } => Some((base_head, head)),
        }
    }

    pub(crate) fn acknowledgement_remote_objects(
        &self,
        acknowledgement: &crate::database::ExactProtocolObject<super::store_commit::StoreAck>,
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreOutboundError> {
        let reference = self.commit.acknowledgement().ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "prepared acknowledgement operation has no exact acknowledgement ref".to_string(),
            )
        })?;
        if reference.object != acknowledgement.object
            || reference.ack_hash != acknowledgement.value.ack_hash()
            || acknowledgement.value.to_bytes() != acknowledgement.bytes
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "prepared acknowledgement operation differs from its exact acknowledgement object"
                    .to_string(),
            ));
        }
        let authority =
            super::remote_object::RemoteObjectRecord::candidate_activated_store_acknowledgement(
                reference.clone(),
                acknowledgement.bytes.clone(),
                acknowledgement.prepared.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        self.retained_authority_remote_objects(vec![authority])
    }

    pub(crate) fn membership_control_remote_objects(
        &self,
        wraps: &[super::wrapped_store_key::PreparedWrappedStoreKey],
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreOutboundError> {
        let control = self.commit.control().ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "membership-wrap ownership requires a signed Store control".to_string(),
            )
        })?;
        let expected = control.introduced_wrapped_keys();
        if expected.is_empty() {
            return Err(StoreOutboundError::InvalidOutbound(
                "signed Store control introduces no membership wraps".to_string(),
            ));
        }
        if expected.len() != wraps.len()
            || expected
                .iter()
                .zip(wraps)
                .any(|(reference, prepared)| **reference != prepared.reference)
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "prepared membership wraps differ from the signed Store control".to_string(),
            ));
        }
        let authorities = wraps
            .iter()
            .map(|prepared| {
                let value = prepared.validate().map_err(StoreObjectError::from)?;
                let canonical = serde_json::to_vec(&value).map_err(|error| {
                    StoreOutboundError::InvalidOutbound(format!(
                        "serialize prepared membership wrap: {error}"
                    ))
                })?;
                super::remote_object::RemoteObjectRecord::candidate_activated_membership_control_wrapped_store_key(
                    prepared.reference.clone(),
                    canonical,
                    prepared.object.stored_bytes().to_vec(),
                    self.reference.clone(),
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.retained_authority_remote_objects(authorities)
    }

    pub(crate) fn retained_authority_remote_objects(
        &self,
        authorities: Vec<super::remote_object::RemoteObjectRecord>,
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreOutboundError> {
        if authorities.is_empty() {
            return Err(StoreOutboundError::InvalidOutbound(
                "Store operation has no retained authority objects".to_string(),
            ));
        }
        let mut authority_ids = std::collections::BTreeSet::new();
        for authority in &authorities {
            authority
                .validate()
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            if !matches!(authority, super::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                if matches!(&record.state, super::remote_object::RetainedAuthorityObjectState::Prepared { ownership }
                    if ownership.pending == std::collections::BTreeSet::from([self.reference.clone()])))
            {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Store operation retained authority has different candidate ownership"
                        .to_string(),
                ));
            }
            if !authority_ids.insert(authority.object_id()) {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Store operation repeats a retained authority object".to_string(),
                ));
            }
        }
        let mut objects = self.candidate_remote_objects()?;
        objects.extend(authorities);
        Ok(objects)
    }

    pub(crate) fn merge_head_ref(&self) -> Option<StoreDeviceHeadRef> {
        match &self.publication {
            StoreOperationPublication::MergeConcurrent { head, prepared, .. } => {
                Some(StoreDeviceHeadRef {
                    head_hash: head.head_hash(),
                    object: prepared.reference().clone(),
                })
            }
            StoreOperationPublication::Serial { .. } => None,
        }
    }

    pub(crate) fn serial_base_head(&self) -> Option<&VersionedObject> {
        match &self.publication {
            StoreOperationPublication::MergeConcurrent { .. } => None,
            StoreOperationPublication::Serial { base_head, .. } => Some(base_head),
        }
    }

    pub(crate) fn adopt_serial_base_head(
        &mut self,
        observed: VersionedObject,
    ) -> Result<(), StoreOutboundError> {
        let StoreOperationPublication::Serial { base_head, .. } = &mut self.publication else {
            return Err(StoreOutboundError::InvalidOutbound(
                "Merge Store operation cannot adopt a Serial head receipt".to_string(),
            ));
        };
        if *base_head == observed {
            return Err(StoreOutboundError::InvalidOutbound(
                "Serial Store operation already carries the observed head receipt".to_string(),
            ));
        }
        *base_head = observed;
        Ok(())
    }

    pub(crate) fn adopt_merge_head(
        &mut self,
        winner: StoreDeviceHead,
        prepared: PreparedExactObject,
    ) -> Result<(), StoreOutboundError> {
        let StoreOperationPublication::MergeConcurrent {
            head: current,
            prepared: current_prepared,
            history_summary,
            ..
        } = &mut self.publication
        else {
            return Err(StoreOutboundError::InvalidOutbound(
                "Serial Store operation cannot adopt a Merge head".to_string(),
            ));
        };
        if winner.commit != self.reference
            || prepared.reference().slot() != current_prepared.reference().slot()
            || prepared.reference() == current_prepared.reference()
            || winner.author_registration != current.author_registration
            || winner.successor.activation != current.successor.activation
            || winner.successor.predecessor != current.successor.predecessor
            || winner.history_summary != history_summary.digest()
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "alternate Merge head differs from the prepared activation point".to_string(),
            ));
        }
        *current = winner;
        *current_prepared = prepared;
        Ok(())
    }

    pub(crate) fn attach_merge_membership_proof(
        &mut self,
        storage: &dyn SyncStorage,
        publication: &super::invite::PreparedMembershipPublication,
        resolution_value: Option<&super::membership::StoreMembershipConflictResolution>,
        identity_signer: &UserKeypair,
    ) -> Result<(), StoreOutboundError> {
        super::invite::validate_prepared_publication(publication)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let StoreOperationPublication::MergeConcurrent {
            head,
            prepared,
            history_summary,
            ..
        } = &mut self.publication
        else {
            return Err(StoreOutboundError::InvalidOutbound(
                "Serial Store operation cannot carry a Merge membership proof".to_string(),
            ));
        };
        let Some(StoreControl::MergeMembership { transition }) = self.commit.control() else {
            return Err(StoreOutboundError::InvalidOutbound(
                "Merge membership proof accompanies another Store control".to_string(),
            ));
        };
        if !transition.matches_head(&publication.head, &publication.head_ref)
            || publication.entry_ref != transition.body.entry
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "Merge membership proof differs from its signed Store transition".to_string(),
            ));
        }
        let resolution = match &publication.entry.change {
            super::membership::MembershipChange::ResolutionActivation { resolution } => {
                let value = resolution_value.ok_or_else(|| {
                    StoreOutboundError::InvalidOutbound(
                        "Merge resolution activation lacks its exact resolution proof".to_string(),
                    )
                })?;
                if value.resolution_ref(resolution.object.clone()) != *resolution {
                    return Err(StoreOutboundError::InvalidOutbound(
                        "Merge resolution proof differs from its exact reference".to_string(),
                    ));
                }
                (Some(resolution.clone()), Some(value.clone()))
            }
            _ if resolution_value.is_none() => (None, None),
            _ => {
                return Err(StoreOutboundError::InvalidOutbound(
                    "non-resolution membership proof carries a resolution".to_string(),
                ))
            }
        };
        history_summary.membership_proofs.insert(
            self.reference.clone(),
            super::store_commit::RetainedMergeMembershipProof {
                commit: self.reference.clone(),
                commit_value: self.commit.clone(),
                announcement: None,
                entry: publication.entry_ref.clone(),
                entry_value: publication.entry.clone(),
                head: publication.head_ref.clone(),
                head_value: publication.head.clone(),
                resolution: resolution.0,
                resolution_value: resolution.1,
            },
        );
        history_summary
            .membership_floor
            .advance(
                publication.entry_ref.coord.clone(),
                &publication.head.body.resolutions,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        history_summary
            .validate_shape()
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let author = history_summary
            .registrations
            .get(&head.author_registration.device_id)
            .filter(|registration| registration.reference == head.author_registration)
            .ok_or_else(|| {
                StoreOutboundError::InvalidOutbound(
                    "Merge membership proof lacks its exact author registration".to_string(),
                )
            })?;
        let device_signer = author
            .value
            .device_signer(identity_signer)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let replacement = StoreDeviceHead::signed(
            head.store_root_hash,
            head.author_registration.clone(),
            head.commit.clone(),
            history_summary.digest(),
            head.successor.clone(),
            &device_signer,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let context = ProtocolObjectContext::signed_plaintext(
            head.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let prefix = head_slot_prefix(
            &head.author_registration.device_id.to_string(),
            head.commit.coord.sequence(),
        );
        *prepared = storage
            .prepare_protocol_object(
                &context,
                prepared.reference().slot().clone(),
                &prefix,
                replacement.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        *head = replacement;
        self.validate_closed_shape()
            .map_err(StoreOutboundError::InvalidOutbound)?;
        Ok(())
    }
}
