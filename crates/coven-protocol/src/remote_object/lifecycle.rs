use super::identity::*;
use super::nonactivation::*;
use super::ownership::*;
use super::*;

/// A Store head names the commit it publishes. The record carries that commit
/// reference, extracted from the head's bytes once when the record was built,
/// so a load can check that the head still belongs to a candidate that owns it
/// without parsing anything.
fn validate_head_commit_ownership(
    head_commit: &StoreBatchCommitRef,
    state: &RetainedAuthorityObjectState,
) -> Result<(), RemoteObjectRecordError> {
    let owns_head_commit = match state {
        RetainedAuthorityObjectState::Prepared { ownership } => {
            ownership.pending.len() == 1 && ownership.pending.contains(head_commit)
        }
        RetainedAuthorityObjectState::UploadedVerified { ownership } => {
            ownership.pending.contains(head_commit) || ownership.activated.contains(head_commit)
        }
        RetainedAuthorityObjectState::CleanupPending { former_candidates }
        | RetainedAuthorityObjectState::AbsentVerified { former_candidates }
        | RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
            ensure_candidate_nonactivation(former_candidates, head_commit).is_ok()
        }
    };
    if owns_head_commit {
        Ok(())
    } else {
        Err(RemoteObjectRecordError::CandidateOwnerMismatch)
    }
}

impl RemoteObjectRecord {
    /// Everything this record asserts about itself that does not need its
    /// payloads: where those payloads live, that the identity is the one the
    /// record is filed under, and that its ownership state holds together.
    ///
    /// Byte agreement is [`Self::validate_payload`]'s job, and it is checked
    /// where bytes arrive from outside this device's own durable state, rather
    /// than on every load. Identity and payload cannot drift apart afterwards:
    /// neither hash mutates across transitions, and the two domain changes that
    /// do happen re-wrap the same reference.
    pub fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        self.validate_payload_placement()?;
        match self {
            Self::CandidateCommit(record) => match &record.state {
                CandidateCommitState::Prepared | CandidateCommitState::UploadedVerified => {}
                CandidateCommitState::CleanupPending { proof }
                | CandidateCommitState::AbsentVerified { proof } => {
                    proof.validate()?;
                }
            },
            Self::CandidateExclusive(record) => {
                if record.identity.family != record.identity.domain.family()
                    || record.identity.object != *record.identity.domain.object()
                {
                    return Err(RemoteObjectRecordError::StoredReferenceMismatch);
                }
                record.state.validate()?;
            }
            Self::RetainedAuthority(record) => {
                if matches!(
                    record.state,
                    RetainedAuthorityObjectState::CleanupPending { .. }
                        | RetainedAuthorityObjectState::AbsentVerified { .. }
                ) && !matches!(
                    record.identity.domain,
                    RetainedAuthorityObjectDomain::StoreMembershipResolution { .. }
                ) {
                    return Err(RemoteObjectRecordError::DomainMismatch);
                }
                if let RetainedAuthorityObjectDomain::DeviceHead { head_commit, .. } =
                    &record.identity.domain
                {
                    validate_head_commit_ownership(head_commit, &record.state)?;
                }
                record.state.validate()?;
            }
            Self::SharedLiveSet(record) => {
                if let SharedLiveSetObjectDomain::StoredBlob = &record.identity.domain {
                    let locator_bytes = record
                        .payloads
                        .carried_locator_bytes()
                        .ok_or(RemoteObjectRecordError::PayloadPlacement)?;
                    validate_semantic_hash(record.identity.semantic_hash, locator_bytes)?;
                    let locator = crate::blob::locator::BlobLocator::parse(locator_bytes)?;
                    crate::blob::locator::StoredBlobRef::new(
                        locator,
                        record.identity.object.clone(),
                    )?;
                }
                record.state.validate()?;
            }
        }
        Ok(())
    }

    /// Refuse a record whose payloads sit somewhere its domain cannot put them.
    ///
    /// This is what makes the carry-set structural rather than conventional: a
    /// stored blob's row travels inside published images and carries its
    /// locator, and no other domain may, because no other domain's payload
    /// would arrive with the row.
    fn validate_payload_placement(&self) -> Result<(), RemoteObjectRecordError> {
        let placed = match self {
            Self::CandidateCommit(_) => {
                matches!(self.payloads(), RemoteObjectPayloads::SpooledInline)
            }
            Self::CandidateExclusive(record) => match &record.identity.domain {
                CandidateExclusiveObjectDomain::CircleBootstrapImage { .. } => {
                    matches!(record.payloads, RemoteObjectPayloads::SpooledExternal)
                }
                // A package this device sealed uploads its own ciphertext; one
                // it observed and activated was sealed elsewhere.
                CandidateExclusiveObjectDomain::StorePackage { .. }
                | CandidateExclusiveObjectDomain::CirclePackage { .. } => {
                    !matches!(record.payloads, RemoteObjectPayloads::RowBlob { .. })
                }
                _ => matches!(record.payloads, RemoteObjectPayloads::SpooledInline),
            },
            Self::RetainedAuthority(record) => {
                matches!(record.payloads, RemoteObjectPayloads::SpooledInline)
            }
            Self::SharedLiveSet(record) => match &record.identity.domain {
                SharedLiveSetObjectDomain::StoredBlob => {
                    matches!(record.payloads, RemoteObjectPayloads::RowBlob { .. })
                }
                SharedLiveSetObjectDomain::StoreSnapshotImage { .. }
                | SharedLiveSetObjectDomain::CircleBootstrapImage { .. } => {
                    matches!(record.payloads, RemoteObjectPayloads::SpooledExternal)
                }
                SharedLiveSetObjectDomain::StorePackage { .. }
                | SharedLiveSetObjectDomain::CirclePackage { .. } => {
                    !matches!(record.payloads, RemoteObjectPayloads::RowBlob { .. })
                }
            },
        };
        if placed {
            Ok(())
        } else {
            Err(RemoteObjectRecordError::PayloadPlacement)
        }
    }

    /// Check this record's identity against the plaintext it names — the whole
    /// domain parse, its signature verifications, and its agreement with the
    /// reference.
    ///
    /// Called where bytes enter from somewhere this device does not already
    /// trust: a constructor handed the payload, a pull that parsed it off the
    /// wire. Reading back this device's own durable state does not run it —
    /// neither loading the row nor reading the spool file the row names, which
    /// is named for the digest of its own contents and was fixed by this
    /// record's identity when it was built.
    pub fn validate_payload(
        &self,
        canonical_semantic_bytes: &[u8],
    ) -> Result<(), RemoteObjectRecordError> {
        self.validate()?;
        match self {
            Self::CandidateCommit(record) => {
                validate_semantic_hash(record.semantic_hash, canonical_semantic_bytes)?;
                let commit: crate::store_commit::StoreBatchCommit =
                    serde_json::from_slice(canonical_semantic_bytes)?;
                record.identity.verify_commit(&commit)?;
                match &record.state {
                    CandidateCommitState::Prepared | CandidateCommitState::UploadedVerified => {}
                    CandidateCommitState::CleanupPending { proof }
                    | CandidateCommitState::AbsentVerified { proof } => {
                        proof.validate_for(&record.identity, &commit)?;
                    }
                }
            }
            Self::CandidateExclusive(record) => {
                validate_candidate_exclusive_identity(&record.identity, canonical_semantic_bytes)?;
            }
            Self::RetainedAuthority(record) => {
                validate_retained_authority_identity(&record.identity, canonical_semantic_bytes)?;
            }
            Self::SharedLiveSet(record) => {
                record
                    .identity
                    .validate_semantic(canonical_semantic_bytes)?;
                match &record.identity.domain {
                    SharedLiveSetObjectDomain::StoredBlob
                    | SharedLiveSetObjectDomain::StoreSnapshotImage { .. }
                    | SharedLiveSetObjectDomain::CircleBootstrapImage { .. } => {}
                    SharedLiveSetObjectDomain::StorePackage { reference } => {
                        validate_package_reference(
                            reference,
                            None,
                            canonical_semantic_bytes,
                            &record.identity.object,
                        )?;
                    }
                    SharedLiveSetObjectDomain::CirclePackage { reference } => {
                        validate_package_reference(
                            &reference.package,
                            Some(reference),
                            canonical_semantic_bytes,
                            &record.identity.object,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn into_activated(
        self,
        commit: &StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let activated = match self {
            Self::CandidateCommit(record) => {
                if &record.identity != commit
                    || !matches!(record.state, CandidateCommitState::UploadedVerified)
                {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                }
                Self::RetainedAuthority(RetainedAuthorityRecord {
                    identity: RetainedAuthorityObjectRef {
                        semantic_hash: record.semantic_hash,
                        object: record.identity.object.clone(),
                        domain: RetainedAuthorityObjectDomain::Commit {
                            reference: record.identity,
                        },
                    },
                    payloads: record.payloads,
                    state: RetainedAuthorityObjectState::UploadedVerified {
                        ownership: CandidateOwnership {
                            pending: BTreeSet::new(),
                            activated: BTreeSet::from([commit.clone()]),
                            nonactivated: Vec::new(),
                        },
                    },
                })
            }
            Self::CandidateExclusive(record) => {
                let CandidateObjectState::UploadedVerified { ownership } = &record.state else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                if ownership.pending.len() != 1 || !ownership.pending.contains(commit) {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                }
                if let Some(domain) = record.identity.domain.shared_destination() {
                    Self::SharedLiveSet(SharedObjectRecord {
                        identity: SharedLiveSetObjectRef {
                            domain,
                            semantic_hash: record.identity.semantic_hash,
                            object: record.identity.object,
                        },
                        payloads: record.payloads,
                        state: OwnedObjectState::UploadedVerified {
                            ownership: SharedObjectOwnership {
                                pending: BTreeSet::new(),
                                activated: BTreeSet::from([SharedObjectOwner::StoreCommit(
                                    commit.clone(),
                                )]),
                                nonactivated: Vec::new(),
                            },
                        },
                    })
                } else if let Some(domain) = record.identity.domain.retained_destination() {
                    Self::RetainedAuthority(RetainedAuthorityRecord {
                        identity: RetainedAuthorityObjectRef {
                            domain,
                            semantic_hash: record.identity.semantic_hash,
                            object: record.identity.object,
                        },
                        payloads: record.payloads,
                        state: RetainedAuthorityObjectState::UploadedVerified {
                            ownership: CandidateOwnership {
                                pending: BTreeSet::new(),
                                activated: BTreeSet::from([commit.clone()]),
                                nonactivated: Vec::new(),
                            },
                        },
                    })
                } else {
                    return Err(RemoteObjectRecordError::DomainMismatch);
                }
            }
            Self::RetainedAuthority(mut record) => {
                let RetainedAuthorityObjectState::UploadedVerified { ownership } =
                    &mut record.state
                else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                if ownership.pending.remove(commit) {
                    ownership.activated.insert(commit.clone());
                } else if !ownership.activated.contains(commit) {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                }
                Self::RetainedAuthority(record)
            }
            Self::SharedLiveSet(mut record) => {
                match &mut record.state {
                    OwnedObjectState::UploadedVerified { ownership } => {
                        if ownership.pending.remove(commit) {
                            ownership
                                .activated
                                .insert(SharedObjectOwner::StoreCommit(commit.clone()));
                        } else if !ownership
                            .activated
                            .contains(&SharedObjectOwner::StoreCommit(commit.clone()))
                        {
                            return Err(RemoteObjectRecordError::InvalidActivation);
                        }
                    }
                    OwnedObjectState::Prepared { .. } => {
                        return Err(RemoteObjectRecordError::InvalidActivation);
                    }
                    OwnedObjectState::RetirementPending { .. } => {
                        return Err(RemoteObjectRecordError::InvalidActivation);
                    }
                }
                Self::SharedLiveSet(record)
            }
        };
        activated.validate()?;
        Ok(activated)
    }

    pub fn into_observed_activated(
        mut self,
        commit: &StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        self.mark_uploaded_verified()?;
        self.into_activated(commit)
    }

    pub fn mark_uploaded_verified(&mut self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::CandidateCommit(record) => match record.state {
                CandidateCommitState::Prepared => {
                    record.state = CandidateCommitState::UploadedVerified;
                }
                CandidateCommitState::UploadedVerified => {}
                CandidateCommitState::CleanupPending { .. }
                | CandidateCommitState::AbsentVerified { .. } => {
                    return Err(RemoteObjectRecordError::InvalidUploadTransition);
                }
            },
            Self::CandidateExclusive(record) => match &record.state {
                CandidateObjectState::Prepared { ownership } => {
                    record.state = CandidateObjectState::UploadedVerified {
                        ownership: ownership.clone(),
                    };
                }
                CandidateObjectState::UploadedVerified { .. } => {}
                CandidateObjectState::CleanupPending { .. }
                | CandidateObjectState::AbsentVerified { .. } => {
                    return Err(RemoteObjectRecordError::InvalidUploadTransition);
                }
            },
            Self::RetainedAuthority(record) => match &record.state {
                RetainedAuthorityObjectState::Prepared { ownership } => {
                    record.state = RetainedAuthorityObjectState::UploadedVerified {
                        ownership: CandidateOwnership {
                            pending: ownership.pending.clone(),
                            activated: BTreeSet::new(),
                            nonactivated: ownership.nonactivated.clone(),
                        },
                    };
                }
                RetainedAuthorityObjectState::UploadedVerified { .. } => {}
                RetainedAuthorityObjectState::CleanupPending { .. }
                | RetainedAuthorityObjectState::AbsentVerified { .. }
                | RetainedAuthorityObjectState::UncreatedVerified { .. } => {
                    return Err(RemoteObjectRecordError::InvalidUploadTransition);
                }
            },
            Self::SharedLiveSet(record) => match &record.state {
                OwnedObjectState::Prepared { ownership } => {
                    record.state = OwnedObjectState::UploadedVerified {
                        ownership: SharedObjectOwnership {
                            pending: ownership.pending.clone(),
                            activated: BTreeSet::new(),
                            nonactivated: ownership.nonactivated.clone(),
                        },
                    };
                }
                OwnedObjectState::UploadedVerified { .. } => {}
                OwnedObjectState::RetirementPending { .. } => {
                    return Err(RemoteObjectRecordError::InvalidUploadTransition);
                }
            },
        }
        self.validate()
    }

    pub fn add_retained_authority_candidate(
        &mut self,
        candidate: StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::RetainedAuthority(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let RetainedAuthorityObjectState::UploadedVerified { ownership } = &mut record.state else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        if ownership.activated.contains(&candidate)
            || ownership
                .nonactivated
                .iter()
                .map(CandidateNonactivation::reference)
                .collect::<Result<BTreeSet<_>, _>>()?
                .contains(&candidate)
            || !ownership.pending.insert(candidate)
        {
            return Err(RemoteObjectRecordError::OverlappingOwnership);
        }
        self.validate()
    }

    pub fn merge_retained_authority_activation(
        &mut self,
        expected: &Self,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::RetainedAuthority(expected) = expected else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let RetainedAuthorityObjectState::UploadedVerified {
            ownership: expected_ownership,
        } = &expected.state
        else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        if !expected_ownership.pending.is_empty()
            || !expected_ownership.nonactivated.is_empty()
            || expected_ownership.activated != BTreeSet::from([owner.clone()])
        {
            return Err(RemoteObjectRecordError::InvalidActivation);
        }
        match self {
            Self::CandidateExclusive(current) => {
                if current.identity.domain.retained_destination()
                    != Some(expected.identity.domain.clone())
                    || current.identity.semantic_hash != expected.identity.semantic_hash
                    || current.identity.object != expected.identity.object
                    || current.payloads != expected.payloads
                {
                    return Err(RemoteObjectRecordError::StoredReferenceMismatch);
                }
                let owns_activation = match &current.state {
                    CandidateObjectState::Prepared { ownership }
                    | CandidateObjectState::UploadedVerified { ownership } => {
                        ownership.pending == BTreeSet::from([owner.clone()])
                    }
                    CandidateObjectState::CleanupPending { .. }
                    | CandidateObjectState::AbsentVerified { .. } => false,
                };
                if !owns_activation {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                }
                let mut activated = Self::CandidateExclusive(current.clone());
                activated.mark_uploaded_verified()?;
                *self = activated.into_activated(owner)?;
            }
            Self::RetainedAuthority(current) => {
                if current.identity != expected.identity || current.payloads != expected.payloads {
                    return Err(RemoteObjectRecordError::StoredReferenceMismatch);
                }
                if matches!(current.state, RetainedAuthorityObjectState::Prepared { .. }) {
                    self.mark_uploaded_verified()?;
                }
                let Self::RetainedAuthority(current) = self else {
                    unreachable!("retained authority remains in its domain")
                };
                let RetainedAuthorityObjectState::UploadedVerified { ownership } =
                    &mut current.state
                else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                ownership.pending.remove(owner);
                ownership.activated.insert(owner.clone());
            }
            Self::CandidateCommit(_) | Self::SharedLiveSet(_) => {
                return Err(RemoteObjectRecordError::DomainMismatch);
            }
        }
        self.validate()
    }

    pub fn begin_candidate_nonactivation(
        &mut self,
        nonactivation: CandidateNonactivation,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        self.begin_candidate_nonactivation_with_head_evidence(
            nonactivation,
            CandidateHeadEvidence::OccupiedByProof,
        )
    }

    pub fn begin_candidate_nonactivation_with_verified_head_nonactivation(
        &mut self,
        nonactivation: CandidateNonactivation,
        head_nonactivation: &VerifiedCandidateHeadNonactivation,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        if matches!(
            self,
            Self::RetainedAuthority(RetainedAuthorityRecord {
                state: RetainedAuthorityObjectState::UncreatedVerified { .. },
                ..
            })
        ) {
            return self.reconcile_verified_candidate_head_nonactivation(
                &nonactivation,
                head_nonactivation,
            );
        }
        self.begin_candidate_nonactivation_with_head_evidence(
            nonactivation,
            CandidateHeadEvidence::Verified(head_nonactivation),
        )
    }

    fn reconcile_verified_candidate_head_nonactivation(
        &mut self,
        nonactivation: &CandidateNonactivation,
        head_nonactivation: &VerifiedCandidateHeadNonactivation,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        nonactivation.validate()?;
        let candidate = nonactivation.reference()?;
        let Self::RetainedAuthority(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        if !matches!(
            record.identity.domain,
            RetainedAuthorityObjectDomain::DeviceHead { .. }
        ) || head_nonactivation.candidate != candidate
            || head_nonactivation.head.object() != &record.identity.object
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "fresh excluded-author head evidence names another prepared object".to_string(),
            ));
        }
        let RetainedAuthorityObjectState::UncreatedVerified { former_candidates } = &record.state
        else {
            return Err(RemoteObjectRecordError::InvalidProof(
                "fresh excluded-author head evidence reached a nonterminal head state".to_string(),
            ));
        };
        let mut stored = None;
        for former_candidate in former_candidates {
            if former_candidate.reference()? == candidate {
                stored = Some(former_candidate);
                break;
            }
        }
        let stored = stored.ok_or(RemoteObjectRecordError::CandidateOwnerMismatch)?;
        if stored != nonactivation {
            return Err(RemoteObjectRecordError::InvalidProof(
                "fresh excluded-author head evidence differs from its durable proof".to_string(),
            ));
        }
        match &head_nonactivation.head {
            VerifiedCandidateHead::ExactCandidateAbsent { .. } => Ok(None),
            VerifiedCandidateHead::ExactLateCandidate { .. } => {
                ProtocolInertObject::new(record.identity.clone(), former_candidates.clone())
                    .map(Some)
            }
        }
    }

    fn begin_candidate_nonactivation_with_head_evidence(
        &mut self,
        nonactivation: CandidateNonactivation,
        head_evidence: CandidateHeadEvidence<'_>,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        nonactivation.validate()?;
        let candidate = nonactivation.reference()?;
        if matches!(head_evidence, CandidateHeadEvidence::Verified(_))
            && !matches!(
                self,
                Self::RetainedAuthority(RetainedAuthorityRecord {
                    identity: RetainedAuthorityObjectRef {
                        domain: RetainedAuthorityObjectDomain::DeviceHead { .. },
                        ..
                    },
                    ..
                })
            )
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "candidate head absence evidence reached a non-head object".to_string(),
            ));
        }
        match self {
            Self::CandidateCommit(record) => {
                if record.identity != candidate {
                    return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                }
                match &record.state {
                    CandidateCommitState::Prepared | CandidateCommitState::UploadedVerified => {
                        record.state = CandidateCommitState::CleanupPending {
                            proof: nonactivation.proof,
                        };
                    }
                    CandidateCommitState::CleanupPending { .. }
                    | CandidateCommitState::AbsentVerified { .. } => {}
                }
            }
            Self::CandidateExclusive(record) => match &mut record.state {
                CandidateObjectState::Prepared { ownership }
                | CandidateObjectState::UploadedVerified { ownership } => {
                    if !ownership.pending.remove(&candidate) {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    }
                    ownership.nonactivated.push(nonactivation);
                    if ownership.pending.is_empty() {
                        record.state = CandidateObjectState::CleanupPending {
                            former_candidates: ownership.nonactivated.clone(),
                        };
                    }
                }
                CandidateObjectState::CleanupPending { former_candidates }
                | CandidateObjectState::AbsentVerified { former_candidates } => {
                    ensure_candidate_nonactivation(former_candidates, &candidate)?;
                }
            },
            Self::RetainedAuthority(record) => match &mut record.state {
                RetainedAuthorityObjectState::Prepared { ownership } => {
                    let RetainedAuthorityObjectDomain::DeviceHead { .. } = &record.identity.domain
                    else {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    };
                    match &head_evidence {
                        CandidateHeadEvidence::OccupiedByProof => {
                            let CandidateNonactivationProof::MergeWinner { winner_head } =
                                &nonactivation.proof
                            else {
                                return Err(RemoteObjectRecordError::InvalidProof(
                                    "a prepared Store head requires winner or verified-absence evidence"
                                        .to_string(),
                                ));
                            };
                            if winner_head.object.slot() != record.identity.object.slot()
                                || winner_head.object == record.identity.object
                            {
                                return Err(RemoteObjectRecordError::InvalidProof(
                                    "Merge winner does not occupy the prepared head's exact slot"
                                        .to_string(),
                                ));
                            }
                        }
                        CandidateHeadEvidence::Verified(head_nonactivation) => {
                            if !matches!(
                                nonactivation.proof,
                                CandidateNonactivationProof::AuthorExclusion { .. }
                                    | CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
                            ) || head_nonactivation.candidate != candidate
                                || head_nonactivation.head.object() != &record.identity.object
                            {
                                return Err(RemoteObjectRecordError::InvalidProof(
                                    "excluded-author head observation names another prepared object"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    if !ownership.pending.remove(&candidate) {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    }
                    ownership.nonactivated.push(nonactivation);
                    if ownership.pending.is_empty() {
                        match head_evidence {
                            CandidateHeadEvidence::Verified(
                                VerifiedCandidateHeadNonactivation {
                                    head: VerifiedCandidateHead::ExactLateCandidate { .. },
                                    ..
                                },
                            ) => {
                                return ProtocolInertObject::new(
                                    record.identity.clone(),
                                    ownership.nonactivated.clone(),
                                )
                                .map(Some);
                            }
                            CandidateHeadEvidence::OccupiedByProof
                            | CandidateHeadEvidence::Verified(
                                VerifiedCandidateHeadNonactivation {
                                    head: VerifiedCandidateHead::ExactCandidateAbsent { .. },
                                    ..
                                },
                            ) => {
                                record.state = RetainedAuthorityObjectState::UncreatedVerified {
                                    former_candidates: ownership.nonactivated.clone(),
                                };
                            }
                        }
                    }
                }
                RetainedAuthorityObjectState::UploadedVerified { .. } => {
                    if matches!(
                        head_evidence,
                        CandidateHeadEvidence::Verified(VerifiedCandidateHeadNonactivation {
                            head: VerifiedCandidateHead::ExactCandidateAbsent { .. },
                            ..
                        })
                    ) {
                        return Err(RemoteObjectRecordError::InvalidProof(
                            "excluded-author head was verified absent but is marked uploaded"
                                .to_string(),
                        ));
                    }
                    let RetainedAuthorityObjectState::UploadedVerified { ownership } =
                        &record.state
                    else {
                        unreachable!("matched uploaded retained authority")
                    };
                    let mut ownership = ownership.clone();
                    if !ownership.pending.remove(&candidate) {
                        ensure_candidate_nonactivation(&ownership.nonactivated, &candidate)?;
                        return Ok(None);
                    }
                    ownership.nonactivated.push(nonactivation);
                    match uploaded_retained_nonactivation_disposition(
                        &record.identity.domain,
                        ownership,
                    ) {
                        UploadedRetainedNonactivation::Cleanup(former_candidates) => {
                            record.state =
                                RetainedAuthorityObjectState::CleanupPending { former_candidates };
                        }
                        UploadedRetainedNonactivation::Inert(former_candidates) => {
                            return ProtocolInertObject::new(
                                record.identity.clone(),
                                former_candidates,
                            )
                            .map(Some);
                        }
                        UploadedRetainedNonactivation::Retain(ownership) => {
                            record.state =
                                RetainedAuthorityObjectState::UploadedVerified { ownership };
                        }
                    }
                }
                RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
                    if matches!(
                        head_evidence,
                        CandidateHeadEvidence::Verified(VerifiedCandidateHeadNonactivation {
                            head: VerifiedCandidateHead::ExactLateCandidate { .. },
                            ..
                        })
                    ) {
                        return Err(RemoteObjectRecordError::InvalidProof(
                            "excluded-author head is present but is marked uncreated".to_string(),
                        ));
                    }
                    ensure_candidate_nonactivation(former_candidates, &candidate)?;
                }
                RetainedAuthorityObjectState::CleanupPending { former_candidates }
                | RetainedAuthorityObjectState::AbsentVerified { former_candidates } => {
                    ensure_candidate_nonactivation(former_candidates, &candidate)?;
                }
            },
            Self::SharedLiveSet(record) => match &mut record.state {
                OwnedObjectState::Prepared { ownership } => {
                    if !ownership.pending.remove(&candidate) {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    }
                    ownership.nonactivated.push(nonactivation);
                    if ownership.pending.is_empty() {
                        record.state = OwnedObjectState::RetirementPending {
                            former_candidates: ownership.nonactivated.clone(),
                        };
                    }
                }
                OwnedObjectState::UploadedVerified { ownership } => {
                    if !ownership.pending.remove(&candidate) {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    }
                    ownership.nonactivated.push(nonactivation);
                    if ownership.pending.is_empty() && ownership.activated.is_empty() {
                        record.state = OwnedObjectState::RetirementPending {
                            former_candidates: ownership.nonactivated.clone(),
                        };
                    }
                }
                OwnedObjectState::RetirementPending { former_candidates } => {
                    ensure_candidate_nonactivation(former_candidates, &candidate)?;
                }
            },
        }
        self.validate()?;
        Ok(None)
    }

    pub fn cleanup_target(&self) -> Option<&ExactObjectRef> {
        match self {
            Self::CandidateCommit(CandidateCommitRecord {
                state: CandidateCommitState::CleanupPending { .. },
                ..
            })
            | Self::CandidateExclusive(CandidateObjectRecord {
                state: CandidateObjectState::CleanupPending { .. },
                ..
            })
            | Self::RetainedAuthority(RetainedAuthorityRecord {
                state: RetainedAuthorityObjectState::CleanupPending { .. },
                ..
            }) => Some(self.object()),
            _ => None,
        }
    }

    pub fn mark_absent_verified(&mut self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::CandidateCommit(record) => match &record.state {
                CandidateCommitState::CleanupPending { proof } => {
                    record.state = CandidateCommitState::AbsentVerified {
                        proof: proof.clone(),
                    };
                }
                CandidateCommitState::AbsentVerified { .. } => {}
                _ => return Err(RemoteObjectRecordError::InvalidCleanupTransition),
            },
            Self::CandidateExclusive(record) => match &record.state {
                CandidateObjectState::CleanupPending { former_candidates } => {
                    record.state = CandidateObjectState::AbsentVerified {
                        former_candidates: former_candidates.clone(),
                    };
                }
                CandidateObjectState::AbsentVerified { .. } => {}
                _ => return Err(RemoteObjectRecordError::InvalidCleanupTransition),
            },
            Self::RetainedAuthority(record) => match &record.state {
                RetainedAuthorityObjectState::CleanupPending { former_candidates } => {
                    record.state = RetainedAuthorityObjectState::AbsentVerified {
                        former_candidates: former_candidates.clone(),
                    };
                }
                RetainedAuthorityObjectState::AbsentVerified { .. } => {}
                _ => return Err(RemoteObjectRecordError::InvalidCleanupTransition),
            },
            Self::SharedLiveSet(_) => {
                return Err(RemoteObjectRecordError::InvalidCleanupTransition)
            }
        }
        self.validate()
    }

    pub fn candidate_cleanup_complete(
        &self,
        candidate: &StoreBatchCommitRef,
    ) -> Result<bool, RemoteObjectRecordError> {
        self.validate()?;
        let contains =
            |former: &[CandidateNonactivation]| -> Result<bool, RemoteObjectRecordError> {
                former
                    .iter()
                    .map(CandidateNonactivation::reference)
                    .try_fold(false, |found, reference| {
                        reference.map(|reference| found || &reference == candidate)
                    })
            };
        match self {
            Self::CandidateCommit(record) => Ok(&record.identity == candidate
                && matches!(record.state, CandidateCommitState::AbsentVerified { .. })),
            Self::CandidateExclusive(record) => match &record.state {
                CandidateObjectState::Prepared { ownership }
                | CandidateObjectState::UploadedVerified { ownership } => Ok(!ownership
                    .pending
                    .contains(candidate)
                    && contains(&ownership.nonactivated)?),
                CandidateObjectState::CleanupPending { .. } => Ok(false),
                CandidateObjectState::AbsentVerified { former_candidates } => {
                    contains(former_candidates)
                }
            },
            Self::RetainedAuthority(record) => match &record.state {
                RetainedAuthorityObjectState::Prepared { ownership } => Ok(!ownership
                    .pending
                    .contains(candidate)
                    && contains(&ownership.nonactivated)?),
                RetainedAuthorityObjectState::UploadedVerified { ownership } => {
                    Ok(!ownership.pending.contains(candidate)
                        && !ownership.activated.contains(candidate)
                        && contains(&ownership.nonactivated)?)
                }
                RetainedAuthorityObjectState::CleanupPending { .. } => Ok(false),
                RetainedAuthorityObjectState::AbsentVerified { former_candidates } => {
                    contains(former_candidates)
                }
                RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
                    contains(former_candidates)
                }
            },
            Self::SharedLiveSet(record) => match &record.state {
                OwnedObjectState::Prepared { ownership } => Ok(!ownership
                    .pending
                    .contains(candidate)
                    && contains(&ownership.nonactivated)?),
                OwnedObjectState::UploadedVerified { ownership } => {
                    Ok(!ownership.pending.contains(candidate)
                        && !ownership
                            .activated
                            .contains(&SharedObjectOwner::StoreCommit(candidate.clone()))
                        && contains(&ownership.nonactivated)?)
                }
                OwnedObjectState::RetirementPending { former_candidates } => {
                    contains(former_candidates)
                }
            },
        }
    }

    pub fn candidate_nonactivation_proof(
        &self,
        candidate: &StoreBatchCommitRef,
    ) -> Result<Option<&CandidateNonactivationProof>, RemoteObjectRecordError> {
        self.validate()?;
        match self {
            Self::CandidateCommit(record) => {
                if &record.identity != candidate {
                    return Ok(None);
                }
                match &record.state {
                    CandidateCommitState::CleanupPending { proof }
                    | CandidateCommitState::AbsentVerified { proof } => Ok(Some(proof)),
                    CandidateCommitState::Prepared | CandidateCommitState::UploadedVerified => {
                        Ok(None)
                    }
                }
            }
            Self::CandidateExclusive(record) => match &record.state {
                CandidateObjectState::Prepared { ownership }
                | CandidateObjectState::UploadedVerified { ownership } => {
                    find_nonactivation_proof(&ownership.nonactivated, candidate)
                }
                CandidateObjectState::CleanupPending { former_candidates }
                | CandidateObjectState::AbsentVerified { former_candidates } => {
                    find_nonactivation_proof(former_candidates, candidate)
                }
            },
            Self::RetainedAuthority(record) => match &record.state {
                RetainedAuthorityObjectState::Prepared { ownership } => {
                    find_nonactivation_proof(&ownership.nonactivated, candidate)
                }
                RetainedAuthorityObjectState::UploadedVerified { ownership } => {
                    find_nonactivation_proof(&ownership.nonactivated, candidate)
                }
                RetainedAuthorityObjectState::CleanupPending { former_candidates }
                | RetainedAuthorityObjectState::AbsentVerified { former_candidates }
                | RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
                    find_nonactivation_proof(former_candidates, candidate)
                }
            },
            Self::SharedLiveSet(record) => match &record.state {
                OwnedObjectState::Prepared { ownership } => {
                    find_nonactivation_proof(&ownership.nonactivated, candidate)
                }
                OwnedObjectState::UploadedVerified { ownership } => {
                    find_nonactivation_proof(&ownership.nonactivated, candidate)
                }
                OwnedObjectState::RetirementPending { former_candidates } => {
                    find_nonactivation_proof(former_candidates, candidate)
                }
            },
        }
    }
}
