use super::*;

impl<'a> StoreCommitVerifier<'a> {
    pub(crate) async fn exact_next_announcement_slot(
        &mut self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        previous: Option<&VerifiedStoreBatchCommit>,
    ) -> Result<
        (
            crate::protocol::objects::ObjectSlot,
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
        let path = self
            .load_exact_announcement_path(
                registration_ref,
                registration,
                previous.map(VerifiedStoreBatchCommit::reference),
            )
            .await?;
        for reference in &path.commits {
            let loaded;
            let verified =
                if let Some(previous) = previous.filter(|commit| reference == commit.reference()) {
                    previous
                } else {
                    loaded = self.load_ref(reference).await?;
                    &loaded
                };
            if verified.reference() != reference
                || verified.author() != registration
                || verified.value().author_registration != *registration_ref
            {
                return Err(StoreError::InvalidOutbound(
                    "verified Store announcement history belongs to another author".to_string(),
                ));
            }
        }
        Ok((path.next_slot, path.accepted_head))
    }

    pub(crate) async fn load_exact_announcement_path(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        previous: Option<&StoreBatchCommitRef>,
    ) -> Result<ExactAnnouncementPath, StoreError> {
        let DeviceStreamAnchor::StoreAnnouncements { first_slot } = &registration.store_commits
        else {
            return Err(StoreError::InvalidOutbound(
                "Merge registration has no Store announcement anchor".to_string(),
            ));
        };
        let Some(target) = previous else {
            return Ok(ExactAnnouncementPath {
                next_slot: first_slot.clone(),
                accepted_head: None,
                commits: Vec::new(),
            });
        };
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
        let activation = registration
            .store_announcement_activation(registration_ref)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
            .activation_id();
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let mut slot = first_slot.clone();
        let mut predecessor: Option<StoreDeviceHeadRef> = None;
        let mut commits = Vec::new();
        for sequence in 1..=target.coord.sequence() {
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
            let is_target = sequence == target.coord.sequence();
            if is_target && head.commit != *target {
                return Err(StoreError::MergeAnnouncementOccupied {
                    expected: Box::new(target.clone()),
                    actual: Box::new(head.commit.clone()),
                });
            }
            commits.push(head.commit.clone());
            let reference = StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object,
            };
            if is_target {
                return Ok(ExactAnnouncementPath {
                    next_slot: head.successor.next_slot.clone(),
                    accepted_head: Some(reference),
                    commits,
                });
            }
            slot = head.successor.next_slot.clone();
            predecessor = Some(reference);
        }
        Err(StoreError::InvalidOutbound(
            "local Store predecessor traversal ended early".to_string(),
        ))
    }

    pub(crate) async fn verify_terminal_candidate_head(
        &mut self,
        candidate: &VerifiedStoreBatchCommit,
        candidate_head: &StoreDeviceHead,
        candidate_head_object: &ExactObjectRef,
    ) -> Result<crate::protocol::remote_object::VerifiedCandidateHead, StorePullError> {
        let storage = self.storage;
        let root = self.root.reference().clone();
        let candidate_ref = candidate.reference();
        let candidate_commit = candidate.value();
        let candidate_author = candidate.author();
        if candidate_head.commit != *candidate_ref
            || candidate_head.author_registration != candidate_commit.author_registration
        {
            return Err(StorePullError::Database(
                "terminal candidate head names another commit or author".to_string(),
            ));
        }
        StoreDeviceHead::parse_at(
            &candidate_head.to_bytes(),
            root.store_root_hash,
            candidate_author,
            candidate_ref,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
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
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let activation = candidate_author
            .store_announcement_activation(&candidate_commit.author_registration)
            .map_err(|error| StorePullError::Database(error.to_string()))?
            .activation_id();
        if candidate_slot != *candidate_head_object.slot()
            || candidate_head.successor.activation != activation
            || candidate_head.successor.predecessor
                != predecessor_head.map(|reference| reference.object)
        {
            return Err(StorePullError::Database(
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
                crate::protocol::remote_object::VerifiedCandidateHead::ExactCandidateAbsent {
                    object: candidate_head_object.clone(),
                },
            ),
            Ok((bytes, object))
                if bytes == candidate_head.to_bytes() && object == *candidate_head_object =>
            {
                Ok(
                    crate::protocol::remote_object::VerifiedCandidateHead::ExactLateCandidate {
                        object: candidate_head_object.clone(),
                    },
                )
            }
            Ok((bytes, object)) => {
                object.verify(&bytes)?;
                let unverified: StoreDeviceHead =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        StorePullError::Database(format!(
                            "parse competing terminal candidate head: {error}"
                        ))
                    })?;
                if object.slot() != candidate_head_object.slot()
                    || unverified.author_registration != candidate_head.author_registration
                    || unverified.commit.coord != candidate_head.commit.coord
                    || unverified.successor != candidate_head.successor
                {
                    return Err(StorePullError::Database(
                        "competing terminal candidate head differs from the exact successor point"
                            .to_string(),
                    ));
                }
                let competing_commit = self.load_ref(&unverified.commit).await?;
                if competing_commit.author() != candidate_author {
                    return Err(StorePullError::Database(
                        "competing terminal candidate belongs to another author".to_string(),
                    ));
                }
                let winner = StoreDeviceHead::parse_at(
                    &bytes,
                    root.store_root_hash,
                    candidate_author,
                    &unverified.commit,
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?;
                if winner != unverified {
                    return Err(StorePullError::Database(
                        "competing terminal candidate head is not authenticated".to_string(),
                    ));
                }
                Ok(
                    crate::protocol::remote_object::VerifiedCandidateHead::ExactCandidateAbsent {
                        object: candidate_head_object.clone(),
                    },
                )
            }
            Err(error) => Err(StorePullError::Storage(error)),
        }
    }

    pub(crate) async fn verify_author_exclusion_nonactivation(
        &mut self,
        locator: &crate::database::AuthorExclusionActivationLocator,
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
        activation_commit: &VerifiedStoreBatchCommit,
        activation_predecessor_state: &ResolvedStoreDeviceState,
        operations: &VerifiedStoreDeviceOperations,
        candidate: &VerifiedStoreBatchCommit,
        candidate_head: &StoreDeviceHead,
        candidate_head_object: &ExactObjectRef,
    ) -> Result<crate::protocol::remote_object::VerifiedCandidateNonactivation, StorePullError>
    {
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
            || !crate::sync::store::owner::verified_history::registration::device_state_has_active_registration(
                activation_predecessor_state,
                &locator.exclusion().proposal.target,
            )
        {
            return Err(StorePullError::Database(
                "author exclusion activation differs from its verified commit and predecessor"
                    .to_string(),
            ));
        }
        let exact_cut = operations
            .exclusions()
            .find_map(|(exclusion, cut)| (exclusion == locator.exclusion()).then_some(cut));
        if exact_cut != Some(&StoreHistoryCut(locator.accepted_cut().clone())) {
            return Err(StorePullError::Database(
                "author exclusion locator differs from the verified outcome cutoff".to_string(),
            ));
        }
        if candidate_head.commit != *candidate_ref
            || candidate_head.author_registration != locator.exclusion().proposal.target
            || candidate_commit.author_registration != candidate_head.author_registration
        {
            return Err(StorePullError::Database(
                "candidate head differs from the excluded author and exact candidate".to_string(),
            ));
        }
        let verified_candidate_head = self
            .verify_terminal_candidate_head(candidate, candidate_head, candidate_head_object)
            .await?;
        let durable = crate::protocol::remote_object::CandidateNonactivation::from_durable_parts(
            candidate_ref,
            candidate_commit,
            crate::protocol::remote_object::CandidateNonactivationProof::AuthorExclusion {
                exclusion: locator.exclusion().clone(),
                accepted_cut: locator.accepted_cut().clone(),
                activation_head: verified_activation_head,
            },
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        crate::protocol::remote_object::VerifiedCandidateNonactivation::from_verified_author_exclusion(
            durable,
            candidate_ref.clone(),
            verified_candidate_head,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))
    }
}
