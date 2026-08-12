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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCandidateHeadNonactivation {
    pub(super) candidate: StoreBatchCommitRef,
    pub(super) head: VerifiedCandidateHead,
}

impl VerifiedCandidateHeadNonactivation {
    pub fn head(&self) -> &VerifiedCandidateHead {
        &self.head
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifiedCandidateHead {
    ExactCandidateAbsent { object: ExactObjectRef },
    ExactLateCandidate { object: ExactObjectRef },
}

impl VerifiedCandidateHead {
    pub fn object(&self) -> &ExactObjectRef {
        match self {
            Self::ExactCandidateAbsent { object } | Self::ExactLateCandidate { object } => object,
        }
    }
}

pub(super) enum CandidateHeadEvidence<'a> {
    OccupiedByProof,
    Verified(&'a VerifiedCandidateHeadNonactivation),
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

    /// Checks the shape of a receipt already admitted through
    /// `VerifiedCandidateNonactivation`; it does not recreate the live observation.
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCandidateNonactivation {
    evidence: Box<VerifiedCandidateNonactivationEvidence>,
}

#[derive(Debug, Clone)]
pub struct VerifiedDependencyRetractionAuthority {
    durable: CandidateNonactivation,
}

impl VerifiedDependencyRetractionAuthority {
    pub fn after_live_authority_check(
        durable: CandidateNonactivation,
    ) -> Result<Self, RemoteObjectRecordError> {
        durable.validate()?;
        if !matches!(
            durable.proof(),
            CandidateNonactivationProof::MergeDependencyRetraction { .. }
        ) {
            return Err(RemoteObjectRecordError::InvalidProof(
                "dependent retraction authority carries another proof family".to_string(),
            ));
        }
        Ok(Self { durable })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum VerifiedCandidateNonactivationEvidence {
    Merge {
        durable: CandidateNonactivation,
        winner_commit: StoreBatchCommitRef,
    },
    AuthorExclusion {
        durable: CandidateNonactivation,
        head_nonactivation: VerifiedCandidateHeadNonactivation,
    },
    MembershipGrantRevocation {
        durable: CandidateNonactivation,
        head_nonactivation: VerifiedCandidateHeadNonactivation,
    },
    DependencyRetraction {
        durable: CandidateNonactivation,
        head_nonactivation: VerifiedCandidateHeadNonactivation,
    },
}

impl VerifiedCandidateNonactivation {
    pub fn from_verified_merge_winner(
        candidate: StoreBatchCommitDeletionTarget,
        winner_head: crate::store_commit::StoreDeviceHeadRef,
        winner_commit: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let value = Self {
            evidence: Box::new(VerifiedCandidateNonactivationEvidence::Merge {
                durable: CandidateNonactivation {
                    candidate,
                    proof: CandidateNonactivationProof::MergeWinner { winner_head },
                },
                winner_commit,
            }),
        };
        value.durable().validate()?;
        Ok(value)
    }

    pub fn from_verified_author_exclusion(
        durable: CandidateNonactivation,
        candidate: StoreBatchCommitRef,
        head: VerifiedCandidateHead,
    ) -> Result<Self, RemoteObjectRecordError> {
        durable.validate()?;
        if !matches!(
            durable.proof(),
            CandidateNonactivationProof::AuthorExclusion { .. }
        ) || durable.reference()? != candidate
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "verified author-exclusion nonactivation parts disagree".to_string(),
            ));
        }
        let value = Self {
            evidence: Box::new(VerifiedCandidateNonactivationEvidence::AuthorExclusion {
                durable,
                head_nonactivation: VerifiedCandidateHeadNonactivation { candidate, head },
            }),
        };
        Ok(value)
    }

    pub fn from_verified_membership_grant_revocation(
        durable: CandidateNonactivation,
        candidate: StoreBatchCommitRef,
        head: VerifiedCandidateHead,
    ) -> Result<Self, RemoteObjectRecordError> {
        durable.validate()?;
        if !matches!(
            durable.proof(),
            CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
        ) || durable.reference()? != candidate
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "verified membership-revocation nonactivation parts disagree".to_string(),
            ));
        }
        let value = Self {
            evidence: Box::new(
                VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation {
                    durable,
                    head_nonactivation: VerifiedCandidateHeadNonactivation { candidate, head },
                },
            ),
        };
        Ok(value)
    }

    pub fn dependency_retraction(
        dependency: &Self,
        candidate: StoreBatchCommitDeletionTarget,
        author: &crate::store_commit::StoreDeviceRegistration,
        activation_head_object: ExactObjectRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        if !matches!(
            dependency.evidence.as_ref(),
            VerifiedCandidateNonactivationEvidence::AuthorExclusion { .. }
                | VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation { .. }
                | VerifiedCandidateNonactivationEvidence::DependencyRetraction { .. }
        ) {
            return Err(RemoteObjectRecordError::InvalidProof(
                "dependent retraction does not descend from terminal evidence".to_string(),
            ));
        }
        let commit =
            candidate.verify_nonactivation_candidate(author.store_root.store_root_hash, author)?;
        let candidate_reference = commit.reference().clone();
        let dependency_reference = dependency.candidate_reference()?;
        let value = Self {
            evidence: Box::new(
                VerifiedCandidateNonactivationEvidence::DependencyRetraction {
                    durable: CandidateNonactivation {
                        candidate,
                        proof: CandidateNonactivationProof::MergeDependencyRetraction {
                            dependency: dependency_reference,
                            dependency_nonactivation: Box::new(dependency.durable().clone()),
                        },
                    },
                    head_nonactivation: VerifiedCandidateHeadNonactivation {
                        candidate: candidate_reference,
                        head: VerifiedCandidateHead::ExactLateCandidate {
                            object: activation_head_object,
                        },
                    },
                },
            ),
        };
        value.durable().validate()?;
        Ok(value)
    }

    pub fn from_verified_dependency_retraction_authority(
        authority: VerifiedDependencyRetractionAuthority,
        candidate: StoreBatchCommitDeletionTarget,
        author: &crate::store_commit::StoreDeviceRegistration,
        activation_head_object: ExactObjectRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let commit =
            candidate.verify_nonactivation_candidate(author.store_root.store_root_hash, author)?;
        let candidate_reference = commit.reference().clone();
        if authority.durable.candidate != candidate {
            return Err(RemoteObjectRecordError::InvalidProof(
                "verified dependent retraction authority names another candidate".to_string(),
            ));
        }
        let value = Self {
            evidence: Box::new(
                VerifiedCandidateNonactivationEvidence::DependencyRetraction {
                    durable: authority.durable,
                    head_nonactivation: VerifiedCandidateHeadNonactivation {
                        candidate: candidate_reference,
                        head: VerifiedCandidateHead::ExactLateCandidate {
                            object: activation_head_object,
                        },
                    },
                },
            ),
        };
        value.durable().validate()?;
        Ok(value)
    }

    pub fn candidate_reference(&self) -> Result<StoreBatchCommitRef, RemoteObjectRecordError> {
        self.durable().reference()
    }

    pub fn proof(&self) -> &CandidateNonactivationProof {
        &self.durable().proof
    }

    pub fn merge_winner_commit(&self) -> Result<&StoreBatchCommitRef, RemoteObjectRecordError> {
        match self.evidence.as_ref() {
            VerifiedCandidateNonactivationEvidence::Merge { winner_commit, .. } => {
                Ok(winner_commit)
            }
            VerifiedCandidateNonactivationEvidence::AuthorExclusion { .. } => {
                Err(RemoteObjectRecordError::InvalidProof(
                    "author-exclusion nonactivation has no Merge slot winner".to_string(),
                ))
            }
            VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation { .. } => {
                Err(RemoteObjectRecordError::InvalidProof(
                    "membership-grant revocation has no Merge slot winner".to_string(),
                ))
            }
            VerifiedCandidateNonactivationEvidence::DependencyRetraction { .. } => {
                Err(RemoteObjectRecordError::InvalidProof(
                    "dependent retraction has no Merge slot winner".to_string(),
                ))
            }
        }
    }

    pub fn into_durable(self) -> CandidateNonactivation {
        match *self.evidence {
            VerifiedCandidateNonactivationEvidence::Merge { durable, .. }
            | VerifiedCandidateNonactivationEvidence::AuthorExclusion { durable, .. }
            | VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation {
                durable, ..
            }
            | VerifiedCandidateNonactivationEvidence::DependencyRetraction { durable, .. } => {
                durable
            }
        }
    }

    pub fn into_terminal_head_nonactivation(
        self,
    ) -> Result<(CandidateNonactivation, VerifiedCandidateHeadNonactivation), RemoteObjectRecordError>
    {
        match *self.evidence {
            VerifiedCandidateNonactivationEvidence::AuthorExclusion {
                durable,
                head_nonactivation,
            } => Ok((durable, head_nonactivation)),
            VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation {
                durable,
                head_nonactivation,
            } => Ok((durable, head_nonactivation)),
            VerifiedCandidateNonactivationEvidence::DependencyRetraction {
                durable,
                head_nonactivation,
            } => Ok((durable, head_nonactivation)),
            VerifiedCandidateNonactivationEvidence::Merge { .. } => {
                Err(RemoteObjectRecordError::InvalidProof(
                    "candidate nonactivation is not verified by an excluded-author head observation"
                        .to_string(),
                ))
            }
        }
    }

    fn durable(&self) -> &CandidateNonactivation {
        match self.evidence.as_ref() {
            VerifiedCandidateNonactivationEvidence::Merge { durable, .. }
            | VerifiedCandidateNonactivationEvidence::AuthorExclusion { durable, .. }
            | VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation {
                durable, ..
            }
            | VerifiedCandidateNonactivationEvidence::DependencyRetraction { durable, .. } => {
                durable
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateNonactivationProof {
    MergeWinner {
        winner_head: crate::store_commit::StoreDeviceHeadRef,
    },
    AuthorExclusion {
        exclusion: crate::store_commit::StoreDeviceExclusionRef,
        accepted_cut: BTreeMap<crate::causal_grants::AuthorStreamId, StoreBatchCommitRef>,
        activation_head: crate::store_commit::StoreDeviceHeadRef,
    },
    MergeMembershipGrantRevocation {
        grant_id: crate::membership::MembershipGrantId,
        membership: crate::circle_control::StoreMembershipStateRef,
        activation_commit: StoreBatchCommitRef,
        activation_head: crate::store_commit::StoreDeviceHeadRef,
    },
    MergeDependencyRetraction {
        dependency: StoreBatchCommitRef,
        dependency_nonactivation: Box<CandidateNonactivation>,
    },
}

impl CandidateNonactivationProof {
    pub fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::MergeWinner { .. } => Ok(()),
            Self::AuthorExclusion { accepted_cut, .. } => {
                crate::store_commit::validate_store_history_cut(
                    &crate::store_commit::StoreHistoryCut::from_commits(accepted_cut.clone()),
                )
                .map_err(Into::into)
            }
            Self::MergeMembershipGrantRevocation {
                membership,
                activation_commit: _,
                ..
            } => {
                if !membership.heads.windows(2).all(|pair| pair[0] < pair[1])
                    || !membership
                        .resolutions
                        .windows(2)
                        .all(|pair| pair[0] < pair[1])
                {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "membership-grant revocation names a noncanonical membership state"
                            .to_string(),
                    ));
                }
                Ok(())
            }
            Self::MergeDependencyRetraction {
                dependency,
                dependency_nonactivation,
            } => {
                dependency_nonactivation.validate()?;
                if dependency_nonactivation.reference()? != *dependency {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "dependent retraction names another exact dependency".to_string(),
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
            Self::MergeWinner { .. } => Ok(()),
            Self::AuthorExclusion {
                exclusion,
                accepted_cut,
                ..
            } => {
                if commit.author_registration != exclusion.proposal.target {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "author exclusion names another candidate author or policy".to_string(),
                    ));
                }
                let expected_stream =
                    crate::store_commit::StreamActivation::device_authorized_stream_id(
                        commit.store_root_hash,
                        &commit.author_registration,
                        crate::store_commit::StreamAnchorDomain::StoreAnnouncements,
                    );
                let crate::store_commit::StoreCommitCoord {
                    stream_id,
                    sequence,
                } = candidate.coord;
                let beyond_cutoff = match accepted_cut.get(&expected_stream) {
                    Some(reference) => sequence > reference.coord.sequence(),
                    None => true,
                };
                if stream_id != expected_stream || !beyond_cutoff {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "candidate is not strictly beyond its excluded author cutoff".to_string(),
                    ));
                }
                Ok(())
            }
            Self::MergeMembershipGrantRevocation { .. } => Ok(()),
            Self::MergeDependencyRetraction { dependency, .. } => {
                let mut direct = commit
                    .order
                    .dependencies()
                    .values()
                    .collect::<BTreeSet<_>>();
                if let Some(predecessor) = commit.order.predecessor() {
                    direct.insert(predecessor);
                }
                if !direct.contains(dependency) {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "dependent retraction proof is not an exact direct dependency".to_string(),
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

pub(super) fn find_nonactivation_proof<'a>(
    former_candidates: &'a [CandidateNonactivation],
    expected: &StoreBatchCommitRef,
) -> Result<Option<&'a CandidateNonactivationProof>, RemoteObjectRecordError> {
    for candidate in former_candidates {
        if candidate.reference()? == *expected {
            return Ok(Some(&candidate.proof));
        }
    }
    Ok(None)
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
