use super::*;

pub(super) enum UploadedRetainedNonactivation {
    Cleanup(Vec<CandidateNonactivation>),
    Inert(Vec<CandidateNonactivation>),
    Retain(CandidateOwnership),
}

pub(super) fn uploaded_retained_nonactivation_disposition(
    domain: &RetainedAuthorityObjectDomain,
    ownership: CandidateOwnership,
) -> UploadedRetainedNonactivation {
    if !ownership.pending.is_empty() || !ownership.activated.is_empty() {
        return UploadedRetainedNonactivation::Retain(ownership);
    }
    if matches!(
        domain,
        RetainedAuthorityObjectDomain::StoreMembershipResolution { .. }
    ) {
        UploadedRetainedNonactivation::Cleanup(ownership.nonactivated)
    } else {
        UploadedRetainedNonactivation::Inert(ownership.nonactivated)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateNonactivation {
    candidate: StoreBatchCommitDeletionTarget,
    pub(super) proof: CandidateNonactivationProof,
}

impl CandidateNonactivation {
    pub fn candidate(&self) -> &StoreBatchCommitDeletionTarget {
        &self.candidate
    }

    pub fn proof(&self) -> &CandidateNonactivationProof {
        &self.proof
    }

    /// Checks a durable receipt shape; its caller owns the accepted boundary
    /// or competing publication that established nonactivation.
    pub fn validate_durable_shape(
        candidate: &StoreBatchCommitRef,
        commit: &crate::store_commit::StoreBatchCommit,
        proof: CandidateNonactivationProof,
    ) -> Result<(), RemoteObjectRecordError> {
        let value = Self {
            candidate: StoreBatchCommitDeletionTarget {
                coord: candidate.coord.clone(),
                object: candidate.object.clone(),
                canonical_signed_bytes: commit.to_bytes(),
            },
            proof,
        };
        value.validate()
    }

    pub fn from_durable_parts(
        candidate: &StoreBatchCommitRef,
        commit: &crate::store_commit::StoreBatchCommit,
        proof: CandidateNonactivationProof,
    ) -> Result<Self, RemoteObjectRecordError> {
        let value = Self {
            candidate: StoreBatchCommitDeletionTarget {
                coord: candidate.coord.clone(),
                object: candidate.object.clone(),
                canonical_signed_bytes: commit.to_bytes(),
            },
            proof,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        let commit: crate::store_commit::StoreBatchCommit =
            serde_json::from_slice(&self.candidate.canonical_signed_bytes)?;
        if commit.seq() != self.candidate.coord.sequence() {
            return Err(RemoteObjectRecordError::InvalidProof(
                "candidate coordinate differs from its signed bytes".to_string(),
            ));
        }
        let reference = StoreBatchCommitRef::from_commit(
            &commit,
            self.candidate.coord.clone(),
            self.candidate.object.clone(),
        )?;
        self.proof.validate_for(&reference, &commit)
    }

    pub fn reference(&self) -> Result<StoreBatchCommitRef, RemoteObjectRecordError> {
        let commit: crate::store_commit::StoreBatchCommit =
            serde_json::from_slice(&self.candidate.canonical_signed_bytes)?;
        StoreBatchCommitRef::from_commit(
            &commit,
            self.candidate.coord.clone(),
            self.candidate.object.clone(),
        )
        .map_err(Into::into)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn unverified_for_test(
        candidate: StoreBatchCommitDeletionTarget,
        proof: CandidateNonactivationProof,
    ) -> Self {
        Self { candidate, proof }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn proof_mut_for_test(&mut self) -> &mut CandidateNonactivationProof {
        &mut self.proof
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateNonactivationProof {
    AcceptedAbandonment {
        abandonment: StoreBatchCommitDeletionTarget,
    },
    AuthorityRetirement {
        publication: crate::store_commit::StorePublicationRef,
        coverage: crate::store_commit::CommitFrontier,
        creation: crate::membership::MembershipGrantCreationAuthority,
        retirement: crate::membership::MembershipGrantRetirement,
    },
    SnapshotRetirement {
        snapshot: crate::store_commit::AcceptedStoreSnapshotRef,
        coverage: crate::store_commit::CommitFrontier,
    },
}

impl CandidateNonactivationProof {
    pub fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::SnapshotRetirement { coverage, .. }
            | Self::AuthorityRetirement { coverage, .. } => {
                crate::store_commit::CommitFrontier::from_refs(
                    coverage
                        .commits()
                        .iter()
                        .map(|(stream, commit)| (stream.to_string(), commit.clone()))
                        .collect(),
                )?;
                Ok(())
            }
            Self::AcceptedAbandonment { abandonment } => {
                let value: crate::store_commit::StoreBatchCommit =
                    serde_json::from_slice(&abandonment.canonical_signed_bytes)?;
                abandonment
                    .object
                    .verify(&abandonment.canonical_signed_bytes)?;
                StoreBatchCommitRef::from_commit(
                    &value,
                    abandonment.coord.clone(),
                    abandonment.object.clone(),
                )?;
                if value.to_bytes() != abandonment.canonical_signed_bytes
                    || value.abandoned_candidates().is_empty()
                {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "accepted abandonment does not carry canonical candidate manifests".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    pub(super) fn validate_for(
        &self,
        candidate: &StoreBatchCommitRef,
        commit: &crate::store_commit::StoreBatchCommit,
    ) -> Result<(), RemoteObjectRecordError> {
        self.validate()?;
        match self {
            Self::AcceptedAbandonment { abandonment } => {
                let value: crate::store_commit::StoreBatchCommit =
                    serde_json::from_slice(&abandonment.canonical_signed_bytes)?;
                let target = StoreBatchCommitDeletionTarget {
                    coord: candidate.coord.clone(),
                    object: candidate.object.clone(),
                    canonical_signed_bytes: commit.to_bytes(),
                };
                if abandonment.coord != candidate.coord
                    || value.store_root_hash != commit.store_root_hash
                    || value.author_registration != commit.author_registration
                    || value.order.predecessor != commit.order.predecessor
                    || !value
                        .abandoned_candidates()
                        .iter()
                        .any(|manifest| manifest.candidate == target)
                {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "accepted abandonment does not exclude the exact candidate".into(),
                    ));
                }
                Ok(())
            }
            Self::SnapshotRetirement { snapshot, coverage } => {
                let base = crate::store_commit::StorePublicationBase::Snapshot(snapshot.clone());
                base.validate_for_store(commit.store_root_hash)?;
                let later_base = match &commit.publication_base {
                    crate::store_commit::StorePublicationBase::Genesis => true,
                    crate::store_commit::StorePublicationBase::Snapshot(previous) => {
                        previous.publication.position < snapshot.publication.position
                    }
                };
                if !later_base
                    || coverage
                        .commits()
                        .get(&candidate.coord.stream_id)
                        .is_some_and(|covered| {
                            covered.coord.sequence() >= candidate.coord.sequence()
                        })
                {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "snapshot does not retire an unaccepted candidate base".to_string(),
                    ));
                }
                Ok(())
            }
            Self::AuthorityRetirement {
                publication,
                coverage,
                creation,
                ..
            } => {
                publication.validate_slot()?;
                if publication.store_root_hash != commit.store_root_hash
                    || commit.membership_authority.as_ref() != Some(creation)
                    || coverage
                        .commits()
                        .get(&candidate.coord.stream_id)
                        .is_some_and(|tip| tip.coord.sequence() >= candidate.coord.sequence())
                {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "authority retirement does not bound an unaccepted candidate".into(),
                    ));
                }
                Ok(())
            }
        }
    }
}

pub(super) fn validate_nonactivations(
    nonactivated: &[CandidateNonactivation],
) -> Result<(), RemoteObjectRecordError> {
    if nonactivated.is_empty() {
        return Err(RemoteObjectRecordError::EmptyNonactivation);
    }
    let mut references = BTreeSet::new();
    for candidate in nonactivated {
        candidate.validate()?;
        if !references.insert(candidate.reference()?) {
            return Err(RemoteObjectRecordError::OverlappingOwnership);
        }
    }
    Ok(())
}

pub(super) fn ensure_candidate_nonactivation(
    former_candidates: &[CandidateNonactivation],
    expected: &StoreBatchCommitRef,
) -> Result<(), RemoteObjectRecordError> {
    for candidate in former_candidates {
        if candidate.reference()? == *expected {
            return Ok(());
        }
    }
    Err(RemoteObjectRecordError::CandidateNonactivationMissing)
}

pub(super) fn validate_owner_partition<'a>(
    pending: &BTreeSet<StoreBatchCommitRef>,
    activated: impl Iterator<Item = &'a StoreBatchCommitRef>,
    nonactivated: &[CandidateNonactivation],
) -> Result<(), RemoteObjectRecordError> {
    let activated = activated.cloned().collect::<BTreeSet<_>>();
    let mut former = BTreeSet::new();
    for candidate in nonactivated {
        candidate.validate()?;
        former.insert(candidate.reference()?);
    }
    if pending
        .iter()
        .any(|owner| activated.contains(owner) || former.contains(owner))
        || activated.iter().any(|owner| former.contains(owner))
        || former.len() != nonactivated.len()
    {
        return Err(RemoteObjectRecordError::OverlappingOwnership);
    }
    Ok(())
}

pub(super) fn validate_semantic_hash(
    expected: ObjectHash,
    bytes: &[u8],
) -> Result<(), RemoteObjectRecordError> {
    let actual = ObjectHash::digest(bytes);
    if actual != expected {
        return Err(RemoteObjectRecordError::SemanticHashMismatch { expected, actual });
    }
    Ok(())
}
