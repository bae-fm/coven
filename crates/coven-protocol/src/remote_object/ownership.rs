use super::nonactivation::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnedObjectState {
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
pub struct PendingCandidateOwnership {
    pub pending: BTreeSet<StoreBatchCommitRef>,
    pub nonactivated: Vec<CandidateNonactivation>,
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
pub struct SharedObjectOwnership {
    pub pending: BTreeSet<StoreBatchCommitRef>,
    pub activated: BTreeSet<SharedObjectOwner>,
    pub nonactivated: Vec<CandidateNonactivation>,
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
pub struct CandidateOwnership {
    pub pending: BTreeSet<StoreBatchCommitRef>,
    pub activated: BTreeSet<StoreBatchCommitRef>,
    pub nonactivated: Vec<CandidateNonactivation>,
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
pub enum SharedObjectOwner {
    StoreCommit(StoreBatchCommitRef),
    Snapshot(SnapshotObjectOwner),
    RetainedReplay(RetainedReplayOwner),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RetainedReplayOwner {
    Commit {
        commit: StoreBatchCommitRef,
        input_hash: ObjectHash,
    },
}

impl RetainedReplayOwner {
    pub fn commit(&self) -> &StoreBatchCommitRef {
        match self {
            Self::Commit { commit, .. } => commit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshotObjectOwner {
    Store {
        metadata_slot: crate::objects::ObjectSlot,
    },
    Circle {
        activation: StreamActivationId,
        generation: u64,
    },
}

impl RemoteObjectRecord {
    pub fn merge_blob_activation(
        &mut self,
        stored: &crate::blob::locator::StoredBlobRef,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let locator_bytes = stored.locator().to_bytes();
        if record.identity.domain != SharedLiveSetObjectDomain::StoredBlob
            || record.identity.semantic_hash != ObjectHash::digest(&locator_bytes)
            || record.identity.object != *stored.object()
            || record.payloads.carried_locator_bytes() != Some(locator_bytes.as_slice())
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        merge_store_commit_owner(&mut record.state, owner);
        self.validate()
    }

    pub fn merge_package_activation(
        &mut self,
        domain: &SharedLiveSetObjectDomain,
        package: &crate::audience_package::AudiencePackage,
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
            || matches!(record.payloads, RemoteObjectPayloads::RowBlob { .. })
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        merge_store_commit_owner(&mut record.state, owner);
        self.validate()
    }

    pub fn merge_retained_replay_owner(
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

    pub fn remove_all_retained_replay_owners(&mut self) -> Result<(), RemoteObjectRecordError> {
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

    pub fn remove_retained_replay_owner(
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
            SharedLiveSetObjectDomain::StoreMembershipRollup { .. } => None,
            SharedLiveSetObjectDomain::CircleBootstrapImage { .. } => None,
        };
        if let Some((family, domain)) = package_domain {
            let identity = CandidateExclusiveTarget {
                family,
                domain,
                semantic_hash: record.identity.semantic_hash,
                object: record.identity.object.clone(),
            };
            let payloads = record.payloads.clone();
            *self = Self::CandidateExclusive(CandidateObjectRecord {
                identity,
                payloads,
                state: CandidateObjectState::CleanupPending { former_candidates },
            });
        } else {
            record.state = OwnedObjectState::RetirementPending { former_candidates };
        }
        Ok(())
    }

    pub fn merge_snapshot_owner(
        &mut self,
        stored: &crate::blob::locator::StoredBlobRef,
        owner: SnapshotObjectOwner,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let locator_bytes = stored.locator().to_bytes();
        if record.identity.domain != SharedLiveSetObjectDomain::StoredBlob
            || record.identity.semantic_hash != ObjectHash::digest(&locator_bytes)
            || record.identity.object != *stored.object()
            || record.payloads.carried_locator_bytes() != Some(locator_bytes.as_slice())
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        merge_shared_owner(&mut record.state, SharedObjectOwner::Snapshot(owner))?;
        self.validate()
    }

    /// Reassign inherited snapshot ownership in an exported database copy.
    /// Live records keep their owners through the merge methods instead.
    pub fn replace_snapshot_owners_for_image(
        &mut self,
        owner: Option<&SnapshotObjectOwner>,
        pending_store_snapshots: &BTreeSet<crate::objects::ObjectSlot>,
    ) -> Result<(), RemoteObjectRecordError> {
        if let Self::SharedLiveSet(record) = self {
            if let OwnedObjectState::UploadedVerified { ownership } = &mut record.state {
                ownership.activated.retain(|owner| match owner {
                    SharedObjectOwner::Snapshot(SnapshotObjectOwner::Store { metadata_slot }) => {
                        pending_store_snapshots.contains(metadata_slot)
                    }
                    SharedObjectOwner::Snapshot(SnapshotObjectOwner::Circle { .. }) => false,
                    _ => true,
                });
                if let Some(owner) = owner {
                    ownership
                        .activated
                        .insert(SharedObjectOwner::Snapshot(owner.clone()));
                }
            }
        }
        self.validate()
    }

    /// Retire superseded Store snapshot leases at an accepted successor. A
    /// reused object remains owned by that successor; a dropped blob retains
    /// its exact original publication provenance until physical reclaim.
    pub fn retire_superseded_store_snapshot_ownership(
        &mut self,
        metadata_slot: &crate::objects::ObjectSlot,
        superseded: &BTreeSet<crate::objects::ObjectSlot>,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        let current = SharedObjectOwner::Snapshot(SnapshotObjectOwner::Store {
            metadata_slot: metadata_slot.clone(),
        });
        let published_blob = record.identity.domain == SharedLiveSetObjectDomain::StoredBlob
            && ownership
                .activated
                .iter()
                .any(|owner| matches!(owner, SharedObjectOwner::StoreCommit(_)));
        if (!ownership.activated.contains(&current) && !published_blob)
            || superseded.contains(metadata_slot)
        {
            return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
        }
        ownership.activated.retain(|owner| match owner {
            SharedObjectOwner::Snapshot(SnapshotObjectOwner::Store { metadata_slot }) => {
                !superseded.contains(metadata_slot)
            }
            _ => true,
        });
        self.validate()
    }
}
