use super::*;

impl<'a> StoreCommitVerifier<'a> {
    pub(crate) async fn exact_next_announcement_slot(
        &mut self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        previous: Option<&VerifiedStoreBatchCommit>,
    ) -> Result<
        (
            coven_protocol::objects::ObjectSlot,
            Option<StoreDeviceHeadRef>,
        ),
        StoreError,
    > {
        if let Some(previous) = previous {
            if previous.value().author_registration != *registration_ref
                || previous.author() != registration
            {
                return Err(StoreError::InvalidOutbound(
                    "verified Store commit author differs from its announcement registration"
                        .to_string(),
                ));
            }
        }
        let DeviceStreamAnchor::StoreAnnouncements { first_slot } = &registration.store_commits
        else {
            return Err(StoreError::InvalidOutbound(
                "Merge registration has no Store announcement anchor".to_string(),
            ));
        };
        let Some(previous) = previous else {
            return Ok((first_slot.clone(), None));
        };
        let target = previous.reference();
        let expected_stream = StreamActivation::device_authorized_stream_id(
            self.root.reference().store_root_hash,
            registration_ref,
            StreamAnchorDomain::StoreAnnouncements,
        );
        if target.coord.stream_id != expected_stream {
            return Err(StoreError::InvalidOutbound(
                "local predecessor belongs to another Store announcement stream".to_string(),
            ));
        }
        if target.coord.sequence() == 0 {
            return Err(StoreError::InvalidOutbound(
                "local predecessor uses Store announcement sequence zero".to_string(),
            ));
        }
        let target_index = usize::try_from(target.coord.sequence() - 1).map_err(|_| {
            StoreError::InvalidOutbound(
                "local predecessor announcement sequence exceeds the platform address space"
                    .to_string(),
            )
        })?;
        let activation = registration
            .store_announcement_activation(registration_ref)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
            .activation_id();
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        if let Some(accepted) = self
            .accepted_announcements
            .get(registration_ref)
            .and_then(|path| path.get(target_index))
        {
            if accepted.commit != *target {
                return Err(StoreError::MergeAnnouncementOccupied {
                    expected: Box::new(target.clone()),
                    actual: Box::new(accepted.commit.clone()),
                });
            }
            return Ok((accepted.next_slot.clone(), Some(accepted.head.clone())));
        }
        let (start, mut slot, mut predecessor) = match self
            .accepted_announcements
            .get(registration_ref)
            .and_then(|path| path.last().map(|accepted| (path.len(), accepted)))
        {
            Some((length, accepted)) => (
                u64::try_from(length)
                    .ok()
                    .and_then(|sequence| sequence.checked_add(1))
                    .ok_or_else(|| {
                        StoreError::InvalidOutbound(
                            "Store announcement sequence overflow".to_string(),
                        )
                    })?,
                accepted.next_slot.clone(),
                Some(accepted.head.clone()),
            ),
            None => (1, first_slot.clone(), None),
        };
        if start > target.coord.sequence() {
            return Err(StoreError::InvalidOutbound(
                "verified Store announcement path omits an earlier coordinate".to_string(),
            ));
        }
        for sequence in start..=target.coord.sequence() {
            let prefix = head_slot_prefix(&registration.device_id.to_string(), sequence);
            let (bytes, object) = self
                .storage
                .read_protocol_slot(&context, &slot, &prefix)
                .await
                .map_err(StoreObjectError::from)?;
            let verify_bytes = bytes.clone();
            let expected_registration = registration_ref.clone();
            let expected_registration_value = registration.clone();
            let store_root_hash = self.root.reference().store_root_hash;
            let expected_predecessor = predecessor
                .as_ref()
                .map(|reference| reference.object.clone());
            let head = run_blocking_object_verification(
                &prefix,
                &object,
                Box::new(move || {
                    let unverified: StoreDeviceHead =
                        serde_json::from_slice(&verify_bytes).map_err(|error| {
                            StoreProtocolError::Malformed(error.to_string())
                        })?;
                    if unverified.author_registration != expected_registration
                        || unverified.successor.activation != activation
                        || unverified.successor.predecessor != expected_predecessor
                    {
                        return Err(StoreProtocolError::Malformed(format!(
                            "local Store head {sequence} does not extend its exact activated predecessor"
                        )));
                    }
                    StoreDeviceHead::parse_at(
                        &verify_bytes,
                        store_root_hash,
                        &expected_registration_value,
                        &unverified.commit,
                    )
                }),
            )
            .await?;
            if head.commit.coord.stream_id != expected_stream
                || head.commit.coord.sequence() != sequence
            {
                return Err(StoreError::InvalidOutbound(format!(
                    "Store announcement position {sequence} names commit coordinate {:?}",
                    head.commit.coord
                )));
            }
            let is_target = sequence == target.coord.sequence();
            if is_target && head.commit != *target {
                return Err(StoreError::MergeAnnouncementOccupied {
                    expected: Box::new(target.clone()),
                    actual: Box::new(head.commit.clone()),
                });
            }
            let loaded;
            let verified = if is_target {
                previous
            } else {
                loaded = self.load_ref(&head.commit).await?;
                &loaded
            };
            if verified.reference() != &head.commit
                || verified.author() != registration
                || verified.value().author_registration != *registration_ref
            {
                return Err(StoreError::InvalidOutbound(
                    "verified Store announcement history belongs to another author".to_string(),
                ));
            }
            let reference = StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object,
            };
            slot = head.successor.next_slot.clone();
            predecessor = Some(reference.clone());
            self.accepted_announcements
                .entry(registration_ref.clone())
                .or_default()
                .push(VerifiedAcceptedStoreAnnouncement {
                    commit: head.commit.clone(),
                    head: reference,
                    next_slot: slot.clone(),
                });
        }
        let accepted = self
            .accepted_announcements
            .get(registration_ref)
            .and_then(|path| path.get(target_index))
            .ok_or_else(|| {
                StoreError::InvalidOutbound(
                    "local Store predecessor traversal ended early".to_string(),
                )
            })?;
        Ok((accepted.next_slot.clone(), Some(accepted.head.clone())))
    }

    pub(crate) async fn verify_terminal_candidate_head(
        &mut self,
        candidate: &VerifiedStoreBatchCommit,
        candidate_head: &StoreDeviceHead,
        candidate_head_object: &ExactObjectRef,
    ) -> Result<coven_protocol::remote_object::VerifiedCandidateHead, StorePullError> {
        let storage = self.storage;
        let root = self.root.reference().clone();
        let candidate_ref = candidate.reference();
        let candidate_commit = candidate.value();
        let candidate_author = candidate.author();
        if candidate_head.commit != *candidate_ref
            || candidate_head.author_registration != candidate_commit.author_registration
        {
            return Err(StorePullError::InvalidState(
                "terminal candidate head names another commit or author".to_string(),
            ));
        }
        StoreDeviceHead::parse_at(
            &candidate_head.to_bytes(),
            root.store_root_hash,
            candidate_author,
            candidate_ref,
        )
        .map_err(StorePullError::Protocol)?;
        let verified_predecessor = match candidate_commit.order.predecessor() {
            Some(predecessor) => Some(self.load_ref(predecessor).await?),
            None => None,
        };
        candidate_head_object.verify(&candidate_head.to_bytes())?;
        let (candidate_slot, predecessor_head) = self
            .exact_next_announcement_slot(
                &candidate_commit.author_registration,
                candidate_author,
                verified_predecessor.as_ref(),
            )
            .await
            .map_err(|error| StorePullError::Store(Box::new(error)))?;
        let activation = candidate_author
            .store_announcement_activation(&candidate_commit.author_registration)
            .map_err(StorePullError::Protocol)?
            .activation_id();
        if candidate_slot != *candidate_head_object.slot()
            || candidate_head.successor.activation != activation
            || candidate_head.successor.predecessor
                != predecessor_head.map(|reference| reference.object)
        {
            return Err(StorePullError::InvalidState(
                "terminal candidate head does not occupy its exact successor slot".to_string(),
            ));
        }
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let candidate_prefix = head_slot_prefix(
            &candidate_head.author_registration.device_id.to_string(),
            candidate_ref.coord.sequence(),
        );
        match storage
            .read_protocol_slot(&context, &candidate_slot, &candidate_prefix)
            .await
        {
            Err(StorageError::NotFound(_)) => Ok(
                coven_protocol::remote_object::VerifiedCandidateHead::ExactCandidateAbsent {
                    object: candidate_head_object.clone(),
                },
            ),
            Ok((bytes, object))
                if bytes == candidate_head.to_bytes() && object == *candidate_head_object =>
            {
                Ok(
                    coven_protocol::remote_object::VerifiedCandidateHead::ExactLateCandidate {
                        object: candidate_head_object.clone(),
                    },
                )
            }
            Ok((bytes, object)) => {
                object.verify(&bytes)?;
                let unverified: StoreDeviceHead =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        StorePullError::context("parse competing terminal candidate head", error)
                    })?;
                if object.slot() != candidate_head_object.slot()
                    || unverified.author_registration != candidate_head.author_registration
                    || unverified.commit.coord != candidate_head.commit.coord
                    || unverified.successor != candidate_head.successor
                {
                    return Err(StorePullError::InvalidState(
                        "competing terminal candidate head differs from the exact successor point"
                            .to_string(),
                    ));
                }
                let competing_commit = self.load_ref(&unverified.commit).await?;
                if competing_commit.author() != candidate_author {
                    return Err(StorePullError::InvalidState(
                        "competing terminal candidate belongs to another author".to_string(),
                    ));
                }
                let winner = StoreDeviceHead::parse_at(
                    &bytes,
                    root.store_root_hash,
                    candidate_author,
                    &unverified.commit,
                )
                .map_err(StorePullError::Protocol)?;
                if winner != unverified {
                    return Err(StorePullError::InvalidState(
                        "competing terminal candidate head is not authenticated".to_string(),
                    ));
                }
                Ok(
                    coven_protocol::remote_object::VerifiedCandidateHead::ExactCandidateAbsent {
                        object: candidate_head_object.clone(),
                    },
                )
            }
            Err(error) => Err(StorePullError::Storage(error)),
        }
    }

    pub(crate) async fn verify_author_exclusion_nonactivation(
        &mut self,
        locator: &coven_database::AuthorExclusionActivationLocator,
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
        activation_commit: &VerifiedStoreBatchCommit,
        activation_predecessor_state: &ResolvedStoreDeviceState,
        operations: &VerifiedStoreDeviceOperations,
        candidate: &VerifiedStoreBatchCommit,
        candidate_head: &StoreDeviceHead,
        candidate_head_object: &ExactObjectRef,
    ) -> Result<coven_protocol::remote_object::VerifiedCandidateNonactivation, StorePullError> {
        let activation_commit_ref = activation_commit.reference();
        let activation_commit_value = activation_commit.value();
        let candidate_ref = candidate.reference();
        let candidate_commit = candidate.value();
        let verified_activation_head = StoreDeviceHeadRef {
            head_hash: activation_head.head_hash(),
            object: activation_head_object.clone(),
        };
        if activation_head.commit != *activation_commit_ref
            || locator.activation_head() != &verified_activation_head
            || !activation_commit_value
                .device_exclusion_outcomes()
                .contains(&StoreDeviceExclusionOutcomeRef::Excluded(
                    locator.exclusion().clone(),
                ))
            || !crate::sync::store::commit_verification::merge_history::registration::device_state_has_active_registration(
                activation_predecessor_state,
                &locator.exclusion().proposal.target,
            )
        {
            return Err(StorePullError::InvalidState(
                "author exclusion activation differs from its verified commit and predecessor"
                    .to_string(),
            ));
        }
        let exact_cut = operations
            .exclusions()
            .find_map(|(exclusion, cut)| (exclusion == locator.exclusion()).then_some(cut));
        if exact_cut != Some(&StoreHistoryCut(locator.accepted_cut().clone())) {
            return Err(StorePullError::InvalidState(
                "author exclusion locator differs from the verified outcome cutoff".to_string(),
            ));
        }
        if candidate_head.commit != *candidate_ref
            || candidate_head.author_registration != locator.exclusion().proposal.target
            || candidate_commit.author_registration != candidate_head.author_registration
        {
            return Err(StorePullError::InvalidState(
                "candidate head differs from the excluded author and exact candidate".to_string(),
            ));
        }
        let verified_candidate_head = self
            .verify_terminal_candidate_head(candidate, candidate_head, candidate_head_object)
            .await?;
        let durable = coven_protocol::remote_object::CandidateNonactivation::from_durable_parts(
            candidate_ref,
            candidate_commit,
            coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion {
                exclusion: locator.exclusion().clone(),
                accepted_cut: locator.accepted_cut().clone(),
                activation_head: verified_activation_head,
            },
        )
        .map_err(StorePullError::RemoteObject)?;
        coven_protocol::remote_object::VerifiedCandidateNonactivation::from_verified_author_exclusion(
            durable,
            candidate_ref.clone(),
            verified_candidate_head,
        )
        .map_err(StorePullError::RemoteObject)
    }
}
