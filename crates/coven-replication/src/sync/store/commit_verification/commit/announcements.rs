use super::*;

impl<'a> StoreCommitVerifier<'a> {
    pub(crate) fn remember_verified_head(
        &self,
        reference: &StoreDeviceHeadRef,
        verified: VerifiedObject<StoreDeviceHead>,
    ) -> Result<VerifiedObject<StoreDeviceHead>, StoreProtocolError> {
        if verified.semantic_hash != reference.head_hash
            || verified.object != reference.object
            || verified.value.head_hash() != reference.head_hash
        {
            return Err(StoreProtocolError::Malformed(
                "verified Store head differs from its exact reference".to_string(),
            ));
        }
        let mut heads = self
            .verified_heads
            .lock()
            .expect("verified Store device head cache mutex is not poisoned");
        if let Some(existing) = heads.get(reference) {
            if existing.semantic_hash != verified.semantic_hash
                || existing.object != verified.object
                || existing.bytes != verified.bytes
                || existing.value != verified.value
            {
                return Err(StoreProtocolError::Malformed(
                    "one exact Store head reference produced different verified objects"
                        .to_string(),
                ));
            }
            return Ok(existing.clone());
        }
        heads.insert(reference.clone(), verified.clone());
        Ok(verified)
    }

    /// Adopt the announcement position an installed Store snapshot restates for
    /// one author, as the point its chain walk resumes from.
    ///
    /// Adopt the announcement position an installed Store snapshot restates for
    /// one author, as the point its chain walk resumes from.
    ///
    /// It decides how the accepted path is indexed — a covered author's path
    /// holds sequences above the covered tip and nothing at or under it — so it
    /// is admitted before anything is remembered for that author. A walk that
    /// already ran holds those covered positions itself, and since the snapshot
    /// restates the same chain the two have to agree at the tip, after which
    /// what the walk found under it is dropped: the snapshot is now what states
    /// it.
    pub(crate) fn remember_covered_announcement(
        &mut self,
        registration: &StoreDeviceRegistrationRef,
        covered: CoveredStoreAnnouncement,
    ) -> Result<(), StoreProtocolError> {
        if covered.sequence == 0 || covered.commit.coord.sequence() != covered.sequence {
            return Err(StoreProtocolError::Malformed(
                "covered Store announcement differs from its own coordinate".to_string(),
            ));
        }
        match self.covered_announcements.entry(registration.clone()) {
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &covered => {
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(_) => Err(StoreProtocolError::Malformed(
                "one Store announcement stream reports two covered positions".to_string(),
            )),
            std::collections::btree_map::Entry::Vacant(entry) => {
                if let Some(path) = self.accepted_announcements.get_mut(registration) {
                    let covered_length = usize::try_from(covered.sequence).map_err(|_| {
                        StoreProtocolError::Malformed(
                            "covered Store announcement sequence exceeds the platform address \
                             space"
                                .to_string(),
                        )
                    })?;
                    match path.get(covered_length.wrapping_sub(1)) {
                        Some(walked)
                            if walked.commit != covered.commit
                                || walked.head != covered.head
                                || walked.next_slot != covered.next_slot =>
                        {
                            return Err(StoreProtocolError::Malformed(
                                "Store snapshot coverage disagrees with the walked announcement \
                                 chain"
                                    .to_string(),
                            ));
                        }
                        Some(_) => {
                            path.drain(..covered_length);
                        }
                        // The walk stopped under the coverage, so every entry it
                        // found is restated by the snapshot.
                        None => path.clear(),
                    }
                }
                entry.insert(covered);
                Ok(())
            }
        }
    }

    /// The sequence an author's accepted path starts above: everything at or
    /// under it is restated by the installed snapshot and held nowhere else.
    fn covered_through(&self, registration: &StoreDeviceRegistrationRef) -> u64 {
        self.covered_announcements
            .get(registration)
            .map_or(0, |covered| covered.sequence)
    }

    pub(crate) fn covered_announcement_floor(
        &self,
        registration: &StoreDeviceRegistrationRef,
    ) -> u64 {
        self.covered_through(registration)
    }

    /// The commit an author's stream stands at according to the installed
    /// snapshot, for a walk that resumed at the coverage and found nothing
    /// above it.
    pub(crate) fn covered_announcement_commit(
        &self,
        registration: &StoreDeviceRegistrationRef,
    ) -> Option<&StoreBatchCommitRef> {
        self.covered_announcements
            .get(registration)
            .map(|covered| &covered.commit)
    }

    pub(crate) fn remember_accepted_announcement(
        &mut self,
        registration: &StoreDeviceRegistrationRef,
        sequence: u64,
        commit: StoreBatchCommitRef,
        head: StoreDeviceHeadRef,
        next_slot: coven_protocol::objects::ObjectSlot,
    ) -> Result<(), StoreProtocolError> {
        let accepted = VerifiedAcceptedStoreAnnouncement {
            commit,
            head,
            next_slot,
        };
        let covered_through = self.covered_through(registration);
        let index = sequence
            .checked_sub(covered_through)
            .and_then(|offset| offset.checked_sub(1))
            .and_then(|index| usize::try_from(index).ok())
            .ok_or_else(|| {
                StoreProtocolError::Malformed(
                    "Store announcement sequence is at or under its snapshot coverage".to_string(),
                )
            })?;
        let path = self
            .accepted_announcements
            .entry(registration.clone())
            .or_default();
        match path.get(index) {
            Some(existing) if existing == &accepted => Ok(()),
            Some(_) => Err(StoreProtocolError::Malformed(
                "verified Store announcement path disagrees at one coordinate".to_string(),
            )),
            None if index == path.len() => {
                path.push(accepted);
                Ok(())
            }
            None => Err(StoreProtocolError::Malformed(
                "verified Store announcement path has a sequence gap".to_string(),
            )),
        }
    }

    pub(crate) fn accepted_announcement_prefix(
        &self,
        registration: &StoreDeviceRegistrationRef,
        first_slot: &coven_protocol::objects::ObjectSlot,
        maximum_sequence: Option<u64>,
    ) -> Result<VerifiedAcceptedStoreAnnouncementPrefix, StorePullError> {
        let covered = self.covered_announcements.get(registration);
        let covered_through = covered.map_or(0, |covered| covered.sequence);
        // Where a walk starts when nothing above the coverage is accepted yet:
        // the snapshot's own tip, or the stream anchor for an author no
        // snapshot covers.
        let (start_slot, start_predecessor) = match covered {
            Some(covered) => (covered.next_slot.clone(), Some(covered.head.object.clone())),
            None => (first_slot.clone(), None),
        };
        let Some(path) = self.accepted_announcements.get(registration) else {
            return Ok(VerifiedAcceptedStoreAnnouncementPrefix {
                commits: Vec::new(),
                next_slot: start_slot,
                predecessor: start_predecessor,
                next_sequence: covered_through.saturating_add(1),
            });
        };
        let heads = self
            .verified_heads
            .lock()
            .expect("verified Store device head cache mutex is not poisoned");
        let mut commits = Vec::new();
        let mut next_slot = start_slot;
        let mut predecessor = start_predecessor;
        let mut accepted_count = 0u64;
        for (index, accepted) in path.iter().enumerate() {
            let sequence = u64::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .and_then(|offset| offset.checked_add(covered_through))
                .ok_or_else(|| {
                    StorePullError::InvalidState(
                        "verified Store announcement path exceeds the protocol sequence range"
                            .to_string(),
                    )
                })?;
            if maximum_sequence.is_some_and(|maximum| sequence > maximum) {
                break;
            }
            let head = heads.get(&accepted.head).ok_or_else(|| {
                StorePullError::InvalidState(
                    "verified Store announcement path is missing its authenticated head"
                        .to_string(),
                )
            })?;
            let commit = self.commits.get(&accepted.commit).ok_or_else(|| {
                StorePullError::InvalidState(
                    "verified Store announcement path is missing its authenticated commit"
                        .to_string(),
                )
            })?;
            commits.push((
                accepted.head.clone(),
                head.value.clone(),
                accepted.commit.clone(),
                commit.value().clone(),
            ));
            next_slot = accepted.next_slot.clone();
            predecessor = Some(accepted.head.object.clone());
            accepted_count = accepted_count.saturating_add(1);
        }
        let next_sequence = covered_through
            .checked_add(accepted_count)
            .and_then(|sequence| sequence.checked_add(1))
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "verified Store announcement path exceeds the protocol sequence range"
                        .to_string(),
                )
            })?;
        Ok(VerifiedAcceptedStoreAnnouncementPrefix {
            commits,
            next_slot,
            predecessor,
            next_sequence,
        })
    }

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
        self.commits
            .entry(target.clone())
            .or_insert_with(|| previous.clone());
        let covered = self.covered_announcements.get(registration_ref).cloned();
        let covered_through = covered.as_ref().map_or(0, |covered| covered.sequence);
        // The covered tip is the one old position the snapshot answers for: the
        // owner signed that head, so its slot link is a resume point rather
        // than something to be walked to.
        if let Some(covered) = &covered {
            if target.coord.sequence() == covered.sequence {
                if covered.commit != *target {
                    return Err(StoreError::MergeAnnouncementOccupied {
                        expected: Box::new(target.clone()),
                        actual: Box::new(covered.commit.clone()),
                    });
                }
                return Ok((covered.next_slot.clone(), Some(covered.head.clone())));
            }
        }
        // A target under the coverage is asking for a head the snapshot does
        // not carry — it holds one accepted announcement per stream, at the
        // covered tip. The chain is slot-linked, so the only way to that head
        // is from the anchor, and the walk below starts there. This is the join
        // and exclusion paths asking about one old activation, not the
        // per-pull chain walk, which resumes at the coverage in
        // `accepted_announcement_prefix`.
        let under_coverage = target.coord.sequence() < covered_through;
        let target_index = if under_coverage {
            None
        } else {
            Some(
                target
                    .coord
                    .sequence()
                    .checked_sub(covered_through)
                    .and_then(|offset| offset.checked_sub(1))
                    .and_then(|index| usize::try_from(index).ok())
                    .ok_or_else(|| {
                        StoreError::InvalidOutbound(
                            "local predecessor announcement sequence exceeds the platform \
                             address space"
                                .to_string(),
                        )
                    })?,
            )
        };
        let activation = registration
            .store_announcement_activation(registration_ref)
            .map_err(StoreError::from)?
            .activation_id();
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        if let Some(accepted) = match target_index {
            Some(index) => self
                .accepted_announcements
                .get(registration_ref)
                .and_then(|path| path.get(index)),
            None => self
                .covered_walk
                .get(&(registration_ref.clone(), target.coord.sequence())),
        } {
            if accepted.commit != *target {
                return Err(StoreError::MergeAnnouncementOccupied {
                    expected: Box::new(target.clone()),
                    actual: Box::new(accepted.commit.clone()),
                });
            }
            return Ok((accepted.next_slot.clone(), Some(accepted.head.clone())));
        }
        let walked = if under_coverage {
            None
        } else {
            self.accepted_announcements
                .get(registration_ref)
                .and_then(|path| path.last().map(|accepted| (path.len(), accepted)))
        };
        let mut reached = None;
        let (start, mut slot, mut predecessor) = match walked {
            Some((length, accepted)) => (
                u64::try_from(length)
                    .ok()
                    .and_then(|offset| offset.checked_add(covered_through))
                    .and_then(|sequence| sequence.checked_add(1))
                    .ok_or_else(|| {
                        StoreError::InvalidOutbound(
                            "Store announcement sequence overflow".to_string(),
                        )
                    })?,
                accepted.next_slot.clone(),
                Some(accepted.head.clone()),
            ),
            None => match covered.filter(|_| !under_coverage) {
                Some(covered) => (
                    covered.sequence.saturating_add(1),
                    covered.next_slot.clone(),
                    Some(covered.head.clone()),
                ),
                None => (1, first_slot.clone(), None),
            },
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
                            StoreProtocolError::from(error)
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
                object: object.clone(),
            };
            self.remember_verified_head(
                &reference,
                VerifiedObject {
                    value: head.clone(),
                    bytes,
                    semantic_hash: reference.head_hash,
                    object,
                },
            )
            .map_err(StoreError::from)?;
            slot = head.successor.next_slot.clone();
            predecessor = Some(reference.clone());
            if is_target {
                reached = Some((slot.clone(), reference.clone()));
            }
            // The accepted path holds only what stands above the coverage; a
            // walk that ran under it keeps what it verified beside the path so
            // the next question about the same position costs nothing.
            if sequence > covered_through {
                self.remember_accepted_announcement(
                    registration_ref,
                    sequence,
                    head.commit.clone(),
                    reference,
                    slot.clone(),
                )
                .map_err(StoreError::from)?;
            } else {
                self.covered_walk.insert(
                    (registration_ref.clone(), sequence),
                    VerifiedAcceptedStoreAnnouncement {
                        commit: head.commit.clone(),
                        head: reference,
                        next_slot: slot.clone(),
                    },
                );
            }
        }
        if let Some(reached) = reached {
            return Ok((reached.0, Some(reached.1)));
        }
        let accepted = target_index
            .and_then(|index| {
                self.accepted_announcements
                    .get(registration_ref)
                    .and_then(|path| path.get(index))
            })
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
