use super::*;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ActiveStorePublicationOwner {
    StoreWrite(WriteId),
    StoreAcknowledgement,
    DeviceJoin(coven_protocol::store_commit::DeviceJoinAttemptId),
    DeviceExclusion(ObjectHash),
    MembershipMutation,
    OwnerPromotion(coven_protocol::store_commit::OwnerPromotionId),
    Reclaim(ObjectHash),
    CircleOperation(coven_protocol::circle::CircleOperationId),
    OwnerRecovery,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ActiveStorePublicationAttempt {
    AwaitingPreparation {
        write_id: WriteId,
        author_registration: StoreDeviceRegistrationRef,
        coord: StoreCommitCoord,
    },
    Discarding {
        write_id: WriteId,
        author_registration: StoreDeviceRegistrationRef,
        coord: StoreCommitCoord,
    },
    Commit {
        write_id: WriteId,
        author_registration: StoreDeviceRegistrationRef,
        coord: StoreCommitCoord,
        publication: coven_protocol::prepared_commit::PreparedStorePublication,
    },
    MembershipAbandonment {
        candidate: Box<coven_protocol::prepared_commit::PreparedStoreOperationCommit>,
    },
    CompletingCoveredWrite {
        write_id: WriteId,
        position: coven_protocol::write::SnapshotCoveredPosition,
        publication: coven_protocol::prepared_commit::PreparedStorePublication,
    },
    Snapshot {
        publication: coven_protocol::prepared_commit::PreparedStorePublication,
        retired_objects: Vec<coven_protocol::objects::ExactObjectRef>,
    },
    SnapshotSuperseded {
        publication: coven_protocol::prepared_commit::PreparedStorePublication,
        snapshot: PublishedStoreSnapshot,
        retired_objects: Vec<coven_protocol::objects::ExactObjectRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredStoreCandidate {
    pub nonactivation: coven_protocol::remote_object::CandidateNonactivation,
    pub inputs: RetiredStoreCandidateInputs,
    pub publications: Vec<coven_protocol::store_commit::StorePublicationRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RetiredStoreCandidateInputs {
    Write(Vec<PreparedAudienceBlob>),
    Acknowledgement(coven_protocol::store_commit::RetainedVerifiedActivatedAck),
    Membership(coven_protocol::membership_mutation::PreparedMembershipPublication),
}

impl RetiredStoreCandidate {
    pub(crate) fn blobs(&self) -> &[PreparedAudienceBlob] {
        match &self.inputs {
            RetiredStoreCandidateInputs::Write(blobs) => blobs,
            RetiredStoreCandidateInputs::Acknowledgement(_)
            | RetiredStoreCandidateInputs::Membership(_) => &[],
        }
    }

    pub(crate) fn candidate(&self) -> Result<StoreBatchCommitRef, DbError> {
        self.nonactivation.reference().map_err(DbError::from)
    }

    pub(crate) fn objects(&self) -> Result<Vec<coven_protocol::objects::ExactObjectRef>, DbError> {
        self.nonactivation.validate()?;
        let commit: coven_protocol::store_commit::StoreBatchCommit =
            serde_json::from_slice(&self.nonactivation.candidate().canonical_signed_bytes)
                .map_err(|error| DbError::context("retired Store write candidate", error))?;
        let mut objects = crate::candidate_graph_exact_objects(&commit)?
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        objects.insert(self.nonactivation.candidate().object.clone());
        objects.extend(self.blobs().iter().map(|blob| blob.blob().object().clone()));
        if let RetiredStoreCandidateInputs::Membership(publication) = &self.inputs {
            objects.extend(publication.candidate_object_refs(&commit, &self.candidate()?)?);
        }
        if let RetiredStoreCandidateInputs::Acknowledgement(proof) = &self.inputs {
            proof.validate_predecessors()?;
            if commit.acknowledgement() != Some(&proof.acknowledgement.0)
                || proof.activating_commit != self.candidate()?
            {
                return Err(DbError::Message(
                    "retired acknowledgement proof names another candidate".into(),
                ));
            }
            objects.extend(commit.retained_operation_objects()?);
            objects.extend(
                proof
                    .predecessors
                    .iter()
                    .map(|(reference, _)| reference.object.clone()),
            );
        }
        Ok(objects.into_iter().collect())
    }
}

/// The exact conditional Store-publication attempt owned by one local
/// operation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveStorePublication {
    owner: ActiveStorePublicationOwner,
    attempt: ActiveStorePublicationAttempt,
    /// An exact entry whose publication position was won by another entry.
    /// Kept until its remote bytes have been deleted, independently of whether
    /// the same logical write succeeds under the replacement attempt.
    superseded_entry: Option<coven_protocol::store_commit::StorePublicationRef>,
    retired_candidates: Vec<RetiredStoreCandidate>,
}

impl ActiveStorePublication {
    pub fn for_commit(
        owner: ActiveStorePublicationOwner,
        candidate: &coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<Self, DbError> {
        Self::commit(
            owner,
            candidate.commit.write_id.clone(),
            candidate.commit.author_registration.clone(),
            candidate.reference.coord.clone(),
            candidate.publication.clone(),
        )
    }

    pub fn commit(
        owner: ActiveStorePublicationOwner,
        write_id: WriteId,
        author_registration: StoreDeviceRegistrationRef,
        coord: StoreCommitCoord,
        publication: coven_protocol::prepared_commit::PreparedStorePublication,
    ) -> Result<Self, DbError> {
        if matches!(&owner, ActiveStorePublicationOwner::StoreWrite(owner) if owner != &write_id) {
            return Err(DbError::Message(
                "active Store-write owner differs from its logical write".to_string(),
            ));
        }
        let attempt = ActiveStorePublicationAttempt::Commit {
            write_id,
            author_registration,
            coord,
            publication,
        };
        Self::validated(owner, attempt)
    }

    pub fn snapshot(
        publication: coven_protocol::prepared_commit::PreparedStorePublication,
    ) -> Result<Self, DbError> {
        let owner = ActiveStorePublicationOwner::Snapshot;
        let attempt = ActiveStorePublicationAttempt::Snapshot {
            publication,
            retired_objects: Vec::new(),
        };
        Self::validated(owner, attempt)
    }

    fn validated(
        owner: ActiveStorePublicationOwner,
        attempt: ActiveStorePublicationAttempt,
    ) -> Result<Self, DbError> {
        let publication = match &attempt {
            ActiveStorePublicationAttempt::AwaitingPreparation { .. }
            | ActiveStorePublicationAttempt::Discarding { .. }
            | ActiveStorePublicationAttempt::CompletingCoveredWrite { .. }
            | ActiveStorePublicationAttempt::SnapshotSuperseded { .. } => {
                return Err(DbError::Message(
                    "an awaiting reservation has no prepared publication".to_string(),
                ));
            }
            ActiveStorePublicationAttempt::Commit { publication, .. }
            | ActiveStorePublicationAttempt::Snapshot { publication, .. } => publication,
            ActiveStorePublicationAttempt::MembershipAbandonment { candidate } => {
                &candidate.publication
            }
        };
        let payload_matches = match (&attempt, &publication.entry.payload) {
            (
                ActiveStorePublicationAttempt::Commit {
                    author_registration,
                    coord,
                    ..
                },
                coven_protocol::store_commit::StorePublicationPayload::Commit(reference),
            ) => {
                &publication.entry.author_registration == author_registration
                    && &reference.coord == coord
            }
            (
                ActiveStorePublicationAttempt::Snapshot { .. },
                coven_protocol::store_commit::StorePublicationPayload::Snapshot(_),
            ) => true,
            (
                ActiveStorePublicationAttempt::MembershipAbandonment { candidate },
                coven_protocol::store_commit::StorePublicationPayload::Commit(reference),
            ) => {
                owner == ActiveStorePublicationOwner::MembershipMutation
                    && reference == &candidate.reference
                    && publication.entry.author_registration == candidate.commit.author_registration
                    && !candidate.commit.abandoned_candidates().is_empty()
            }
            _ => false,
        };
        if !payload_matches {
            return Err(DbError::Message(
                "active Store publication source differs from its exact attempt".to_string(),
            ));
        }
        publication
            .reference()
            .map_err(|error| DbError::context("active Store publication attempt", error))?;
        Ok(Self {
            owner,
            attempt,
            superseded_entry: None,
            retired_candidates: Vec::new(),
        })
    }

    pub fn owner(&self) -> &ActiveStorePublicationOwner {
        &self.owner
    }

    pub(crate) fn author_registration(&self) -> &StoreDeviceRegistrationRef {
        match &self.attempt {
            ActiveStorePublicationAttempt::AwaitingPreparation {
                author_registration,
                ..
            }
            | ActiveStorePublicationAttempt::Discarding {
                author_registration,
                ..
            }
            | ActiveStorePublicationAttempt::Commit {
                author_registration,
                ..
            } => author_registration,
            ActiveStorePublicationAttempt::MembershipAbandonment { candidate } => {
                &candidate.commit.author_registration
            }
            ActiveStorePublicationAttempt::CompletingCoveredWrite { position, .. } => {
                &position.author_registration
            }
            ActiveStorePublicationAttempt::Snapshot { publication, .. }
            | ActiveStorePublicationAttempt::SnapshotSuperseded { publication, .. } => {
                &publication.entry.author_registration
            }
        }
    }

    pub fn attempt(
        &self,
    ) -> Result<&coven_protocol::prepared_commit::PreparedStorePublication, DbError> {
        match &self.attempt {
            ActiveStorePublicationAttempt::AwaitingPreparation { .. } => Err(DbError::Message(
                "reserved Store write is awaiting candidate preparation".to_string(),
            )),
            ActiveStorePublicationAttempt::Discarding { .. } => Err(DbError::Message(
                "reserved Store write is being discarded".to_string(),
            )),
            ActiveStorePublicationAttempt::Commit { publication, .. }
            | ActiveStorePublicationAttempt::Snapshot { publication, .. }
            | ActiveStorePublicationAttempt::SnapshotSuperseded { publication, .. }
            | ActiveStorePublicationAttempt::CompletingCoveredWrite { publication, .. } => {
                Ok(publication)
            }
            ActiveStorePublicationAttempt::MembershipAbandonment { candidate } => {
                Ok(&candidate.publication)
            }
        }
    }

    pub fn covered_write_position(
        &self,
    ) -> Option<&coven_protocol::write::SnapshotCoveredPosition> {
        match &self.attempt {
            ActiveStorePublicationAttempt::CompletingCoveredWrite { position, .. } => {
                Some(position)
            }
            _ => None,
        }
    }

    pub(crate) fn begin_covered_write_completion(
        &self,
        position: coven_protocol::write::SnapshotCoveredPosition,
    ) -> Result<Self, DbError> {
        let ActiveStorePublicationAttempt::Commit {
            write_id,
            author_registration,
            coord,
            publication,
        } = &self.attempt
        else {
            return Err(DbError::Message(
                "covered completion requires a prepared Store write".into(),
            ));
        };
        if self.owner != ActiveStorePublicationOwner::StoreWrite(write_id.clone())
            || position.author_registration != *author_registration
            || position.coord != *coord
            || !self.retired_candidates.is_empty()
        {
            return Err(DbError::Message(
                "covered completion differs from its reserved write or retains candidate cleanup"
                    .into(),
            ));
        }
        let mut completed = self.clone();
        completed.attempt = ActiveStorePublicationAttempt::CompletingCoveredWrite {
            write_id: write_id.clone(),
            position,
            publication: publication.clone(),
        };
        Ok(completed)
    }

    pub fn is_awaiting_preparation(&self) -> bool {
        matches!(
            self.attempt,
            ActiveStorePublicationAttempt::AwaitingPreparation { .. }
        )
    }

    pub fn is_discarding(&self) -> bool {
        matches!(
            self.attempt,
            ActiveStorePublicationAttempt::Discarding { .. }
        )
    }

    pub(crate) fn begin_discard(&self) -> Result<Self, DbError> {
        let ActiveStorePublicationAttempt::AwaitingPreparation {
            write_id,
            author_registration,
            coord,
        } = &self.attempt
        else {
            return Err(DbError::Message(
                "discard requires a write whose old candidate has been retired".to_string(),
            ));
        };
        if self.owner != ActiveStorePublicationOwner::StoreWrite(write_id.clone()) {
            return Err(DbError::Message(
                "discard reservation differs from its Store-write owner".to_string(),
            ));
        }
        let mut discarded = self.clone();
        discarded.attempt = ActiveStorePublicationAttempt::Discarding {
            write_id: write_id.clone(),
            author_registration: author_registration.clone(),
            coord: coord.clone(),
        };
        Ok(discarded)
    }

    pub fn retired_candidates(&self) -> &[RetiredStoreCandidate] {
        &self.retired_candidates
    }

    pub fn retired_snapshot_objects(&self) -> &[coven_protocol::objects::ExactObjectRef] {
        match &self.attempt {
            ActiveStorePublicationAttempt::Snapshot {
                retired_objects, ..
            }
            | ActiveStorePublicationAttempt::SnapshotSuperseded {
                retired_objects, ..
            } => retired_objects,
            _ => &[],
        }
    }

    pub(crate) fn retain_snapshot_cleanup(
        &mut self,
        objects: Vec<coven_protocol::objects::ExactObjectRef>,
    ) -> Result<(), DbError> {
        let ActiveStorePublicationAttempt::Snapshot {
            retired_objects, ..
        } = &mut self.attempt
        else {
            return Err(DbError::Message(
                "snapshot cleanup has another publication owner".into(),
            ));
        };
        if !retired_objects.is_empty() {
            return Err(DbError::Message(
                "snapshot cleanup is already pending".into(),
            ));
        }
        *retired_objects = objects;
        Ok(())
    }

    pub(crate) fn complete_snapshot_cleanup(&mut self) -> Result<(), DbError> {
        let retired_objects = match &mut self.attempt {
            ActiveStorePublicationAttempt::Snapshot {
                retired_objects, ..
            }
            | ActiveStorePublicationAttempt::SnapshotSuperseded {
                retired_objects, ..
            } => retired_objects,
            _ => {
                return Err(DbError::Message(
                    "snapshot cleanup has another publication owner".into(),
                ));
            }
        };
        if retired_objects.is_empty() {
            return Err(DbError::Message("snapshot has no pending cleanup".into()));
        }
        retired_objects.clear();
        Ok(())
    }

    pub fn superseding_snapshot(&self) -> Option<&PublishedStoreSnapshot> {
        match &self.attempt {
            ActiveStorePublicationAttempt::SnapshotSuperseded { snapshot, .. } => Some(snapshot),
            _ => None,
        }
    }

    pub(crate) fn supersede_snapshot(
        &self,
        snapshot: PublishedStoreSnapshot,
        retired_objects: Vec<coven_protocol::objects::ExactObjectRef>,
    ) -> Result<Self, DbError> {
        let ActiveStorePublicationAttempt::Snapshot {
            publication,
            retired_objects: pending,
        } = &self.attempt
        else {
            return Err(DbError::Message(
                "only a pending snapshot request can be superseded".into(),
            ));
        };
        if !pending.is_empty() || self.owner != ActiveStorePublicationOwner::Snapshot {
            return Err(DbError::Message(
                "snapshot supersession has unfinished candidate cleanup".into(),
            ));
        }
        let mut superseded = self.clone();
        superseded.attempt = ActiveStorePublicationAttempt::SnapshotSuperseded {
            publication: publication.clone(),
            snapshot,
            retired_objects,
        };
        Ok(superseded)
    }

    pub(crate) fn await_preparation(
        &self,
        cleanup: RetiredStoreCandidate,
    ) -> Result<Self, DbError> {
        if self.covered_write_position().is_some() {
            return Err(DbError::Message(
                "covered write completion cannot return to preparation".into(),
            ));
        }
        let Some((write_id, author_registration, coord)) = self.commit_reservation() else {
            return Err(DbError::Message(
                "snapshot publication cannot reserve a Store write".to_string(),
            ));
        };
        let candidate = cleanup.candidate()?;
        let owner_matches = match (&self.owner, &cleanup.inputs) {
            (
                ActiveStorePublicationOwner::StoreWrite(owner),
                RetiredStoreCandidateInputs::Write(_),
            ) => owner == write_id,
            (
                ActiveStorePublicationOwner::MembershipMutation,
                RetiredStoreCandidateInputs::Membership(_),
            ) => matches!(
                cleanup.nonactivation.proof(),
                coven_protocol::remote_object::CandidateNonactivationProof::AuthorityRetirement { .. }
            ) && self.retired_candidates.is_empty(),
            _ => false,
        };
        if !owner_matches
            || candidate.coord != *coord
            || self.attempt()?.entry.payload
                != coven_protocol::store_commit::StorePublicationPayload::Commit(candidate.clone())
        {
            return Err(DbError::Message(
                "replacement cleanup differs from the reserved Store write".to_string(),
            ));
        }
        cleanup.objects()?;
        let mut retired_candidates = self.retired_candidates.clone();
        retired_candidates.push(cleanup);
        Ok(Self {
            owner: self.owner.clone(),
            attempt: ActiveStorePublicationAttempt::AwaitingPreparation {
                write_id: write_id.clone(),
                author_registration: author_registration.clone(),
                coord: coord.clone(),
            },
            superseded_entry: None,
            retired_candidates,
        })
    }

    pub(crate) fn complete_retired_candidate_cleanup(&mut self) -> Result<(), DbError> {
        if self.retired_candidates.is_empty() {
            return Err(DbError::Message(
                "reserved write has no retired candidate cleanup".to_string(),
            ));
        }
        self.retired_candidates.clear();
        Ok(())
    }

    pub(crate) fn continue_membership_after_abandonment(
        &self,
        cleanup: RetiredStoreCandidate,
    ) -> Result<Self, DbError> {
        let Some((write_id, author_registration, coord)) = self.commit_reservation() else {
            return Err(DbError::Message(
                "membership abandonment has no reserved author position".into(),
            ));
        };
        if self.owner != ActiveStorePublicationOwner::MembershipMutation
            || !matches!(
                self.attempt,
                ActiveStorePublicationAttempt::MembershipAbandonment { .. }
            )
            || cleanup.candidate()?.coord != *coord
            || !matches!(cleanup.inputs, RetiredStoreCandidateInputs::Membership(_))
            || !self.retired_candidates.is_empty()
            || self.superseded_entry.is_some()
        {
            return Err(DbError::Message(
                "membership abandonment differs from its reserved mutation".into(),
            ));
        }
        cleanup.objects()?;
        let sequence = coord.sequence.checked_add(1).ok_or_else(|| {
            DbError::Message("membership continuation exhausts its author sequence".into())
        })?;
        Ok(Self {
            owner: self.owner.clone(),
            attempt: ActiveStorePublicationAttempt::AwaitingPreparation {
                write_id: write_id.clone(),
                author_registration: author_registration.clone(),
                coord: StoreCommitCoord {
                    stream_id: coord.stream_id,
                    sequence,
                },
            },
            superseded_entry: None,
            retired_candidates: vec![cleanup],
        })
    }

    pub fn membership_abandonment(
        &self,
    ) -> Option<&coven_protocol::prepared_commit::PreparedStoreOperationCommit> {
        match &self.attempt {
            ActiveStorePublicationAttempt::MembershipAbandonment { candidate } => Some(candidate),
            _ => None,
        }
    }

    pub(crate) fn begin_membership_abandonment(
        &self,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<Self, DbError> {
        candidate.validate_closed_shape()?;
        if self.owner != ActiveStorePublicationOwner::MembershipMutation
            || !matches!(self.attempt, ActiveStorePublicationAttempt::Commit { .. })
            || self.commit_reservation()
                != Some((
                    &candidate.commit.write_id,
                    &candidate.commit.author_registration,
                    &candidate.reference.coord,
                ))
            || self.superseded_entry.is_some()
            || !self.retired_candidates.is_empty()
        {
            return Err(DbError::Message(
                "membership abandonment changes its operation or retains earlier cleanup".into(),
            ));
        }
        Self::validated(
            self.owner.clone(),
            ActiveStorePublicationAttempt::MembershipAbandonment {
                candidate: Box::new(candidate),
            },
        )
    }

    pub(crate) fn replace_acknowledgement_candidate(
        &self,
        candidate: &coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        cleanup: RetiredStoreCandidate,
    ) -> Result<Self, DbError> {
        if self.owner != ActiveStorePublicationOwner::StoreAcknowledgement
            || self.commit_reservation()
                != Some((
                    &candidate.commit.write_id,
                    &candidate.commit.author_registration,
                    &candidate.reference.coord,
                ))
            || self.attempt()?.entry.payload
                != coven_protocol::store_commit::StorePublicationPayload::Commit(
                    cleanup.candidate()?,
                )
            || !matches!(
                cleanup.inputs,
                RetiredStoreCandidateInputs::Acknowledgement(_)
            )
        {
            return Err(DbError::Message(
                "acknowledgement replacement changes its reserved operation".into(),
            ));
        }
        cleanup.objects()?;
        let mut replacement = self.replace_attempt(candidate.publication.clone())?;
        replacement.retired_candidates.push(cleanup);
        Ok(replacement)
    }

    pub fn commit_reservation(
        &self,
    ) -> Option<(&WriteId, &StoreDeviceRegistrationRef, &StoreCommitCoord)> {
        match &self.attempt {
            ActiveStorePublicationAttempt::AwaitingPreparation {
                write_id,
                author_registration,
                coord,
                ..
            }
            | ActiveStorePublicationAttempt::Discarding {
                write_id,
                author_registration,
                coord,
            }
            | ActiveStorePublicationAttempt::Commit {
                write_id,
                author_registration,
                coord,
                ..
            } => Some((write_id, author_registration, coord)),
            ActiveStorePublicationAttempt::MembershipAbandonment { candidate } => Some((
                &candidate.commit.write_id,
                &candidate.commit.author_registration,
                &candidate.reference.coord,
            )),
            ActiveStorePublicationAttempt::CompletingCoveredWrite {
                write_id, position, ..
            } => Some((write_id, &position.author_registration, &position.coord)),
            ActiveStorePublicationAttempt::Snapshot { .. }
            | ActiveStorePublicationAttempt::SnapshotSuperseded { .. } => None,
        }
    }

    pub fn superseded_entry(&self) -> Option<&coven_protocol::store_commit::StorePublicationRef> {
        self.superseded_entry.as_ref()
    }

    pub fn same_commit_reservation(&self, other: &Self) -> bool {
        self.commit_reservation().is_some()
            && self.owner == other.owner
            && self.commit_reservation() == other.commit_reservation()
            && matches!((self.attempt(), other.attempt()), (Ok(left), Ok(right)) if left.entry.payload == right.entry.payload)
    }

    pub(crate) fn retain_superseded_entry(
        &mut self,
        reference: coven_protocol::store_commit::StorePublicationRef,
    ) -> Result<(), DbError> {
        if self.superseded_entry.is_some() || reference == self.attempt()?.reference()? {
            return Err(DbError::Message(
                "Store publication already owns superseded entry cleanup".to_string(),
            ));
        }
        self.superseded_entry = Some(reference);
        Ok(())
    }

    pub(crate) fn complete_superseded_entry_cleanup(&mut self) -> Result<(), DbError> {
        if self.superseded_entry.take().is_none() {
            return Err(DbError::Message(
                "Store publication has no superseded entry cleanup".to_string(),
            ));
        }
        Ok(())
    }

    pub fn replace_attempt(
        &self,
        attempt: coven_protocol::prepared_commit::PreparedStorePublication,
    ) -> Result<Self, DbError> {
        if self.superseded_entry.is_some() || !self.retired_snapshot_objects().is_empty() {
            return Err(DbError::Message(
                "Store publication must finish superseded entry cleanup before another replacement"
                    .to_string(),
            ));
        }
        let replacement = match &self.attempt {
            ActiveStorePublicationAttempt::Discarding { .. }
            | ActiveStorePublicationAttempt::CompletingCoveredWrite { .. }
            | ActiveStorePublicationAttempt::SnapshotSuperseded { .. } => {
                return Err(DbError::Message(
                    "terminal operation cannot prepare another publication candidate".to_string(),
                ));
            }
            ActiveStorePublicationAttempt::AwaitingPreparation {
                write_id,
                author_registration,
                coord,
            } => ActiveStorePublicationAttempt::Commit {
                write_id: write_id.clone(),
                author_registration: author_registration.clone(),
                coord: coord.clone(),
                publication: attempt,
            },
            ActiveStorePublicationAttempt::Commit {
                write_id,
                author_registration,
                coord,
                ..
            } => ActiveStorePublicationAttempt::Commit {
                write_id: write_id.clone(),
                author_registration: author_registration.clone(),
                coord: coord.clone(),
                publication: attempt,
            },
            ActiveStorePublicationAttempt::Snapshot { .. } => {
                ActiveStorePublicationAttempt::Snapshot {
                    publication: attempt,
                    retired_objects: Vec::new(),
                }
            }
            ActiveStorePublicationAttempt::MembershipAbandonment { candidate } => {
                let mut candidate = candidate.clone();
                candidate.publication = attempt;
                candidate.validate_closed_shape()?;
                ActiveStorePublicationAttempt::MembershipAbandonment { candidate }
            }
        };
        let mut replacement = Self::validated(self.owner.clone(), replacement)?;
        replacement.retired_candidates = self.retired_candidates.clone();
        Ok(replacement)
    }
}

#[derive(Debug, Clone)]
pub struct DurableDeviceRegistration {
    pub device_id: coven_protocol::store_commit::StoreDeviceId,
    pub registration_hash: ObjectHash,
    pub registration_bytes: Vec<u8>,
    pub prepared: PreparedExactObject,
    pub initial_ack_ref: StoreAckRef,
    pub initial_ack: ExactProtocolObject<StoreAck>,
    pub state: LocalDeviceRegistrationState,
}

/// The exact Owner-recovery commit and publication staged before either is
/// published. A retry reopens these same stored bytes instead of resealing the
/// semantic objects into different exact identities.
pub struct OwnerRecoveryPublication {
    pub commit: ExactProtocolObject<coven_protocol::store_commit::VerifiedStoreBatchCommit>,
    pub publication: coven_protocol::prepared_commit::PreparedStorePublication,
    pub history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
}

impl OwnerRecoveryPublication {
    pub fn membership_publication(
        &self,
    ) -> Result<coven_protocol::membership_mutation::PreparedMembershipPublication, DbError> {
        let proof = self
            .history_evidence
            .membership_proof
            .as_ref()
            .ok_or_else(|| {
                DbError::Message("Owner recovery lacks its exact membership activation".into())
            })?;
        let publication = coven_protocol::membership_mutation::PreparedMembershipPublication {
            entry: proof.entry_value.clone(),
            entry_ref: proof.entry.clone(),
            head: proof.head_value.clone(),
            head_ref: proof.head.clone(),
        };
        publication.validate().map_err(|error| {
            DbError::context(
                "Owner recovery membership publication",
                coven_protocol::prepared_commit::PreparedCommitError::from(error),
            )
        })?;
        Ok(publication)
    }

    pub fn remote_objects(
        &self,
    ) -> Result<Vec<coven_protocol::remote_object::ClosedRemoteObject>, DbError> {
        let mut objects = self
            .membership_publication()?
            .candidate_remote_objects(self.commit.value.value(), self.commit.value.reference())?;
        objects.push(
            coven_protocol::remote_object::RemoteObjectRecord::candidate_commit(
                self.commit.value.reference().clone(),
                &self.commit.bytes,
                self.commit.prepared.stored_bytes(),
            )?,
        );
        Ok(objects)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LocalDeviceRegistrationState {
    Prepared,
    RegistrationPublished,
    RegistrationActivated {
        authority: coven_protocol::store_commit::StoreDeviceRegistrationActivation,
    },
    Created,
    Activated {
        authority: coven_protocol::store_commit::StoreDeviceRegistrationActivation,
    },
}

pub type PreparedLocalDeviceRegistrationRow =
    (String, String, Vec<u8>, String, String, Vec<u8>, String);
pub type LocalDeviceRegistrationJournalRow = (
    String,
    String,
    Vec<u8>,
    String,
    String,
    Vec<u8>,
    String,
    String,
);

impl DurableDeviceRegistration {
    pub fn is_activated(&self) -> bool {
        matches!(self.state, LocalDeviceRegistrationState::Activated { .. })
    }
}

#[derive(Debug, Clone)]
pub struct DurableMembershipMutation {
    pub intent_hash: ObjectHash,
    pub plan_bytes: Vec<u8>,
    pub progress_bytes: Vec<u8>,
}

#[derive(Clone, Copy)]
pub enum MembershipMutationActivation {
    WithoutRotation,
    Rotation { generation: u64 },
}

pub struct DurableSnapshotPublication {
    pub reference: StoreSnapshotRef,
    pub meta: ExactProtocolObject<SnapshotMeta>,
    pub publication: coven_protocol::prepared_commit::PreparedStorePublication,
    /// The membership rollup the metadata names. Staged with the snapshot and
    /// uploaded before it, so a published snapshot never names a rollup that is
    /// not at the provider.
    pub rollup: ExactProtocolObject<coven_protocol::store_commit::MembershipRollup>,
    pub image: PreparedProtocolObject<Vec<u8>>,
    pub blobs: Vec<PreparedSnapshotBlob>,
}

pub enum StoreSnapshotPublicationStage {
    Initial,
    Replacing {
        previous: StoreSnapshotRef,
        accepted: crate::AcceptedStorePublicationInterval,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedSnapshotBlob {
    pub bindings: Vec<RowBlobLocatorBinding>,
    pub authority: coven_protocol::audience_package::PackageAudience,
    pub remote: RemoteObjectRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedStoreSnapshot {
    pub reference: StoreSnapshotRef,
    pub meta: SnapshotMeta,
}

pub struct DurableCircleSnapshotPublication {
    pub reference: coven_protocol::store_commit::CircleSnapshotRef,
    pub meta: ExactProtocolObject<coven_protocol::store_commit::CircleSnapshotMeta>,
    pub image: PreparedProtocolObject<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct PublishedCircleSnapshot {
    pub reference: coven_protocol::store_commit::CircleSnapshotRef,
    pub successor_slot: coven_protocol::objects::ObjectSlot,
    pub cut: coven_protocol::store_commit::CommitFrontier,
}
