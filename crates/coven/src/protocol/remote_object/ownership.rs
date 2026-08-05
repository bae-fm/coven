use super::nonactivation::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum OwnedObjectState {
    Prepared {
        ownership: PendingCandidateOwnership,
    },
    UploadedVerified {
        ownership: SharedObjectOwnership,
    },
    RetirementPending {
        former_candidates: Vec<CandidateNonactivation>,
    },
}

impl OwnedObjectState {
    pub(super) fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::Prepared { ownership } => ownership.validate(),
            Self::UploadedVerified { ownership } => ownership.validate(),
            Self::RetirementPending { former_candidates } => {
                validate_nonactivations(former_candidates)
            }
        }
    }
}

pub(super) fn merge_store_commit_owner(state: &mut OwnedObjectState, owner: &StoreBatchCommitRef) {
    match state {
        OwnedObjectState::Prepared { ownership } => {
            let mut pending = ownership.pending.clone();
            pending.remove(owner);
            *state = OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending,
                    activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner.clone())]),
                    nonactivated: ownership.nonactivated.clone(),
                },
            };
        }
        OwnedObjectState::UploadedVerified { ownership } => {
            ownership.pending.remove(owner);
            ownership
                .activated
                .insert(SharedObjectOwner::StoreCommit(owner.clone()));
        }
        OwnedObjectState::RetirementPending { former_candidates } => {
            *state = OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner.clone())]),
                    nonactivated: former_candidates.clone(),
                },
            };
        }
    }
}

pub(super) fn merge_shared_owner(
    state: &mut OwnedObjectState,
    owner: SharedObjectOwner,
) -> Result<(), RemoteObjectRecordError> {
    match state {
        OwnedObjectState::UploadedVerified { ownership } => {
            ownership.activated.insert(owner);
            Ok(())
        }
        OwnedObjectState::RetirementPending { former_candidates } => {
            *state = OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([owner]),
                    nonactivated: former_candidates.clone(),
                },
            };
            Ok(())
        }
        OwnedObjectState::Prepared { .. } => Err(RemoteObjectRecordError::InvalidActivation),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingCandidateOwnership {
    pub(crate) pending: BTreeSet<StoreBatchCommitRef>,
    pub(crate) nonactivated: Vec<CandidateNonactivation>,
}

impl PendingCandidateOwnership {
    pub(super) fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        if self.pending.is_empty() {
            return Err(RemoteObjectRecordError::EmptyPendingOwnership);
        }
        validate_owner_partition(&self.pending, std::iter::empty(), &self.nonactivated)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SharedObjectOwnership {
    pub(crate) pending: BTreeSet<StoreBatchCommitRef>,
    pub(crate) activated: BTreeSet<SharedObjectOwner>,
    pub(crate) nonactivated: Vec<CandidateNonactivation>,
}

impl SharedObjectOwnership {
    pub(super) fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        if self.pending.is_empty() && self.activated.is_empty() {
            Err(RemoteObjectRecordError::EmptyOwnership)
        } else {
            let activated_commits = self.activated.iter().filter_map(|owner| match owner {
                SharedObjectOwner::StoreCommit(commit) => Some(commit),
                SharedObjectOwner::Snapshot(_) | SharedObjectOwner::RetainedReplay(_) => None,
            });
            validate_owner_partition(&self.pending, activated_commits, &self.nonactivated)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateOwnership {
    pub(crate) pending: BTreeSet<StoreBatchCommitRef>,
    pub(crate) activated: BTreeSet<StoreBatchCommitRef>,
    pub(crate) nonactivated: Vec<CandidateNonactivation>,
}

impl CandidateOwnership {
    pub(super) fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        if self.pending.is_empty() && self.activated.is_empty() {
            return Err(RemoteObjectRecordError::EmptyOwnership);
        }
        validate_owner_partition(&self.pending, self.activated.iter(), &self.nonactivated)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SharedObjectOwner {
    StoreCommit(StoreBatchCommitRef),
    Snapshot(SnapshotObjectOwner),
    RetainedReplay(RetainedReplayOwner),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RetainedReplayOwner {
    Commit {
        commit: StoreBatchCommitRef,
        input_hash: ObjectHash,
    },
}

impl RetainedReplayOwner {
    pub(crate) fn commit(&self) -> &StoreBatchCommitRef {
        match self {
            Self::Commit { commit, .. } => commit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotObjectOwner {
    pub(crate) activation: StreamActivationId,
    pub(crate) generation: u64,
}

impl RemoteObjectRecord {
    pub(crate) fn merge_blob_activation(
        &mut self,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let locator_bytes = stored.locator().to_bytes();
        if record.identity.domain != SharedLiveSetObjectDomain::StoredBlob
            || record.identity.semantic_hash != ObjectHash::digest(&locator_bytes)
            || record.identity.object != *stored.object()
            || record.bytes.canonical_semantic_bytes() != locator_bytes
            || record.bytes.stored().object() != stored.object()
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        merge_store_commit_owner(&mut record.state, owner);
        self.validate()
    }

    pub(crate) fn merge_package_activation(
        &mut self,
        domain: &SharedLiveSetObjectDomain,
        package: &crate::protocol::audience_package::AudiencePackage,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        if !matches!(
            domain,
            SharedLiveSetObjectDomain::StorePackage { .. }
                | SharedLiveSetObjectDomain::CirclePackage { .. }
        ) {
            return Err(RemoteObjectRecordError::DomainMismatch);
        }
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let canonical_semantic_bytes = package.to_bytes();
        if &record.identity.domain != domain
            || record.identity.semantic_hash != ObjectHash::digest(&canonical_semantic_bytes)
            || record.identity.object != *domain.package_object()?
            || record.bytes.canonical_semantic_bytes() != canonical_semantic_bytes
            || record.bytes.stored().object() != domain.package_object()?
            || matches!(
                record.bytes.stored(),
                RemoteStoredRepresentation::Blob { .. }
            )
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        merge_store_commit_owner(&mut record.state, owner);
        self.validate()
    }

    pub(crate) fn merge_retained_replay_owner(
        &mut self,
        owner: RetainedReplayOwner,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        ownership
            .activated
            .insert(SharedObjectOwner::RetainedReplay(owner));
        self.validate()
    }

    pub(crate) fn remove_all_retained_replay_owners(
        &mut self,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Ok(());
        };
        let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
            return Ok(());
        };
        ownership
            .activated
            .retain(|owner| !matches!(owner, SharedObjectOwner::RetainedReplay(_)));
        self.validate()
    }

    pub(crate) fn remove_retained_replay_owner(
        &mut self,
        owner: &RetainedReplayOwner,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        if !ownership
            .activated
            .remove(&SharedObjectOwner::RetainedReplay(owner.clone()))
        {
            return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
        }
        self.retire_unowned_shared_live_set()?;
        self.validate()
    }

    fn retire_unowned_shared_live_set(&mut self) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let OwnedObjectState::UploadedVerified { ownership } = &record.state else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        if !ownership.pending.is_empty() || !ownership.activated.is_empty() {
            return Ok(());
        }
        if ownership.nonactivated.is_empty() {
            return Err(RemoteObjectRecordError::EmptyOwnership);
        }
        let former_candidates = ownership.nonactivated.clone();
        let package_domain = match &record.identity.domain {
            SharedLiveSetObjectDomain::StorePackage { reference } => Some((
                reference.candidate_family,
                CandidateExclusiveObjectDomain::StorePackage {
                    reference: reference.clone(),
                },
            )),
            SharedLiveSetObjectDomain::CirclePackage { reference } => Some((
                reference.package.candidate_family,
                CandidateExclusiveObjectDomain::CirclePackage {
                    reference: reference.clone(),
                },
            )),
            SharedLiveSetObjectDomain::StoredBlob => None,
            SharedLiveSetObjectDomain::StoreSnapshotImage { .. } => None,
            SharedLiveSetObjectDomain::CircleBootstrapImage { .. } => None,
        };
        if let Some((family, domain)) = package_domain {
            let identity = CandidateExclusiveTarget {
                family,
                domain,
                semantic_hash: record.identity.semantic_hash,
                object: record.identity.object.clone(),
            };
            let bytes = record.bytes.clone();
            *self = Self::CandidateExclusive(CandidateObjectRecord {
                identity,
                bytes,
                state: CandidateObjectState::CleanupPending { former_candidates },
            });
        } else {
            record.state = OwnedObjectState::RetirementPending { former_candidates };
        }
        Ok(())
    }

    pub(crate) fn retract_activated_candidate(
        &mut self,
        nonactivation: CandidateNonactivation,
        head_nonactivation: Option<&VerifiedCandidateHeadNonactivation>,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        nonactivation.validate()?;
        if !matches!(
            nonactivation.proof,
            CandidateNonactivationProof::AuthorExclusion { .. }
                | CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
        ) {
            return Err(RemoteObjectRecordError::InvalidProof(
                "activated candidate retraction requires terminal author exclusion".to_string(),
            ));
        }
        let candidate = nonactivation.reference()?;
        match self {
            Self::RetainedAuthority(record) => {
                let RetainedAuthorityObjectState::UploadedVerified { ownership } =
                    &mut record.state
                else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                if !ownership.activated.remove(&candidate) {
                    ensure_candidate_nonactivation(&ownership.nonactivated, &candidate)?;
                    return Ok(None);
                }
                ownership.nonactivated.push(nonactivation.clone());
                let is_head = matches!(
                    record.identity.domain,
                    RetainedAuthorityObjectDomain::DeviceHead { .. }
                );
                match (is_head, head_nonactivation) {
                    (true, Some(head_nonactivation))
                        if head_nonactivation.candidate == candidate
                            && matches!(
                                &head_nonactivation.head,
                                VerifiedCandidateHead::ExactLateCandidate { object }
                                    if object == &record.identity.object
                            ) => {}
                    (true, _) => {
                        return Err(RemoteObjectRecordError::InvalidProof(
                            "uploaded retracted candidate head lacks exact presence evidence"
                                .to_string(),
                        ));
                    }
                    (false, None) => {}
                    (false, Some(_)) => {
                        return Err(RemoteObjectRecordError::InvalidProof(
                            "candidate-head evidence reached a non-head activated object"
                                .to_string(),
                        ));
                    }
                }
                if !ownership.pending.is_empty() || !ownership.activated.is_empty() {
                    self.validate()?;
                    return Ok(None);
                }
                if matches!(
                    &record.identity.domain,
                    RetainedAuthorityObjectDomain::Commit { reference }
                        if reference == &candidate
                ) {
                    let bytes = record.bytes.clone();
                    *self = Self::CandidateCommit(CandidateCommitRecord {
                        identity: candidate,
                        bytes,
                        state: CandidateCommitState::CleanupPending {
                            proof: nonactivation.proof,
                        },
                    });
                    self.validate()?;
                    return Ok(None);
                }
                ProtocolInertObject::new(
                    record.identity.clone(),
                    record.bytes.canonical_semantic_bytes().to_vec(),
                    ownership.nonactivated.clone(),
                )
                .map(Some)
            }
            Self::SharedLiveSet(record) => {
                if head_nonactivation.is_some() {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "candidate-head evidence reached a shared activated object".to_string(),
                    ));
                }
                let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                if !ownership
                    .activated
                    .remove(&SharedObjectOwner::StoreCommit(candidate.clone()))
                {
                    ensure_candidate_nonactivation(&ownership.nonactivated, &candidate)?;
                    return Ok(None);
                }
                ownership.nonactivated.push(nonactivation);
                self.retire_unowned_shared_live_set()?;
                self.validate()?;
                Ok(None)
            }
            Self::CandidateCommit(_) | Self::CandidateExclusive(_) => {
                Err(RemoteObjectRecordError::InvalidActivation)
            }
        }
    }

    pub(crate) fn merge_snapshot_owner(
        &mut self,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        owner: SnapshotObjectOwner,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let locator_bytes = stored.locator().to_bytes();
        if record.identity.domain != SharedLiveSetObjectDomain::StoredBlob
            || record.identity.semantic_hash != ObjectHash::digest(&locator_bytes)
            || record.identity.object != *stored.object()
            || record.bytes.canonical_semantic_bytes() != locator_bytes
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        merge_shared_owner(&mut record.state, SharedObjectOwner::Snapshot(owner))?;
        self.validate()
    }
}
