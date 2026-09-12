use super::ownership::*;
use super::*;

impl RemoteObjectRecord {
    pub fn object(&self) -> &ExactObjectRef {
        match self {
            Self::CandidateCommit(record) => &record.identity.object,
            Self::CandidateExclusive(record) => &record.identity.object,
            Self::RetainedAuthority(record) => &record.identity.object,
            Self::SharedLiveSet(record) => &record.identity.object,
        }
    }

    pub fn payloads(&self) -> &RemoteObjectPayloads {
        match self {
            Self::CandidateCommit(record) => &record.payloads,
            Self::CandidateExclusive(record) => &record.payloads,
            Self::RetainedAuthority(record) => &record.payloads,
            Self::SharedLiveSet(record) => &record.payloads,
        }
    }

    /// Where this record's plaintext is. A stored blob carries its locator in
    /// the row, the image domains have no plaintext of their own, and every
    /// other domain names its plaintext in the spool by the identity's semantic
    /// hash.
    pub fn semantic_payload(&self) -> SemanticPayload<'_> {
        if let Some(locator_bytes) = self.payloads().carried_locator_bytes() {
            return SemanticPayload::Carried(locator_bytes);
        }
        match self {
            Self::CandidateCommit(record) => SemanticPayload::Spooled(record.semantic_hash),
            Self::CandidateExclusive(record) => match &record.identity.domain {
                CandidateExclusiveObjectDomain::CircleBootstrapImage { .. } => {
                    SemanticPayload::Absent
                }
                _ => SemanticPayload::Spooled(record.identity.semantic_hash),
            },
            Self::RetainedAuthority(record) => {
                SemanticPayload::Spooled(record.identity.semantic_hash)
            }
            Self::SharedLiveSet(record) => match &record.identity.domain {
                // The images and the membership rollup all went to the provider
                // from the flow that built them, and none is kept here after —
                // so the record claims no spool file, only the reference.
                SharedLiveSetObjectDomain::StoreSnapshotImage { .. }
                | SharedLiveSetObjectDomain::StoreMembershipRollup { .. }
                | SharedLiveSetObjectDomain::CircleBootstrapImage { .. } => SemanticPayload::Absent,
                _ => SemanticPayload::Spooled(record.identity.semantic_hash),
            },
        }
    }

    /// The spooled ciphertext this record uploads, when the ciphertext is its
    /// own to upload.
    pub fn stored_payload(&self) -> Option<ObjectHash> {
        match self.payloads() {
            RemoteObjectPayloads::SpooledInline => Some(self.object().stored_hash()),
            RemoteObjectPayloads::SpooledExternal | RemoteObjectPayloads::RowBlob { .. } => None,
        }
    }

    /// Every spool file this record names. The claim it holds while its row
    /// exists, and what the row's deletion lets go of.
    pub fn payload_claims(&self) -> BTreeSet<ObjectHash> {
        let mut claims = BTreeSet::new();
        if let SemanticPayload::Spooled(hash) = self.semantic_payload() {
            claims.insert(hash);
        }
        claims.extend(self.stored_payload());
        claims
    }

    pub fn object_id(&self) -> ObjectHash {
        remote_object_id(self.object())
    }

    pub fn is_activated_stored_blob(&self) -> bool {
        matches!(
            self,
            Self::SharedLiveSet(record)
                if record.identity.domain == SharedLiveSetObjectDomain::StoredBlob
                    && matches!(
                        &record.state,
                        OwnedObjectState::UploadedVerified { ownership }
                            if !ownership.activated.is_empty()
                    )
        )
    }

    pub fn validate_reclaimable_store_package(
        &self,
        target: &crate::store_commit::StorePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let ownership = self.activated_store_package_ownership(target, activation)?;
        if !ownership.pending.is_empty() || ownership.activated.len() != 1 {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(())
    }

    pub fn validate_reclaimable_circle_package(
        &self,
        target: &crate::store_commit::CirclePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let ownership = self.activated_circle_package_ownership(target, activation)?;
        if !ownership.pending.is_empty() || ownership.activated.len() != 1 {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(())
    }

    /// Whether this record is the activated Store package the target names.
    pub fn validate_activated_store_package(
        &self,
        target: &crate::store_commit::StorePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        self.activated_store_package_ownership(target, activation)
            .map(drop)
    }

    fn activated_store_package_ownership<'a>(
        &'a self,
        target: &crate::store_commit::StorePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<&'a SharedObjectOwnership, RemoteObjectRecordError> {
        self.validate()?;
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        let expected_owner = SharedObjectOwner::StoreCommit(activation.clone());
        if !matches!(
            &record.identity.domain,
            SharedLiveSetObjectDomain::StorePackage { reference } if reference == target
        ) || record.identity.semantic_hash != target.content_hash
            || record.identity.object != target.object
        {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        let OwnedObjectState::UploadedVerified { ownership } = &record.state else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        if !ownership.activated.contains(&expected_owner) {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(ownership)
    }

    /// Whether this record is the activated Circle package the target names.
    pub fn validate_activated_circle_package(
        &self,
        target: &crate::store_commit::CirclePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        self.activated_circle_package_ownership(target, activation)
            .map(drop)
    }

    fn activated_circle_package_ownership<'a>(
        &'a self,
        target: &crate::store_commit::CirclePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<&'a SharedObjectOwnership, RemoteObjectRecordError> {
        self.validate()?;
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        let expected_owner = SharedObjectOwner::StoreCommit(activation.clone());
        if !matches!(
            &record.identity.domain,
            SharedLiveSetObjectDomain::CirclePackage { reference } if reference == target
        ) || record.identity.semantic_hash != target.package.content_hash
            || record.identity.object != target.package.object
        {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        let OwnedObjectState::UploadedVerified { ownership } = &record.state else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        if !ownership.activated.contains(&expected_owner) {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(ownership)
    }

    /// A Circle bootstrap image is reclaimable when its single activating Store
    /// commit is its only surviving owner: no pending activation and exactly one
    /// activated owner. A bootstrap image accretes a per-activating-commit owner,
    /// so more than one means a live successor still references it.
    pub fn validate_reclaimable_circle_bootstrap_image(
        &self,
        image: &crate::store_commit::SnapshotImageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let ownership = self.activated_circle_bootstrap_image_ownership(image, activation)?;
        if !ownership.pending.is_empty() || ownership.activated.len() != 1 {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(())
    }

    /// A snapshot image is reclaimable when the exact snapshot named by the
    /// claim is its only surviving owner, with no pending activation.
    pub fn validate_reclaimable_snapshot_image(
        &self,
        image: &crate::store_commit::SnapshotImageRef,
        owner: &SnapshotObjectOwner,
    ) -> Result<(), RemoteObjectRecordError> {
        let ownership = self.activated_snapshot_image_ownership(image, owner)?;
        if !ownership.pending.is_empty() || ownership.activated.len() != 1 {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(())
    }

    fn activated_snapshot_image_ownership<'a>(
        &'a self,
        image: &crate::store_commit::SnapshotImageRef,
        owner: &SnapshotObjectOwner,
    ) -> Result<&'a SharedObjectOwnership, RemoteObjectRecordError> {
        self.validate()?;
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        if !matches!(
            &record.identity.domain,
            SharedLiveSetObjectDomain::StoreSnapshotImage { reference } if reference == image
        ) || record.identity.semantic_hash != image.image_hash
            || record.identity.object != image.object
            || !matches!(record.payloads, RemoteObjectPayloads::SpooledExternal)
        {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        let OwnedObjectState::UploadedVerified { ownership } = &record.state else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        if !ownership
            .activated
            .contains(&SharedObjectOwner::Snapshot(owner.clone()))
        {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(ownership)
    }

    /// A membership rollup is reclaimable when the exact snapshot named as its
    /// owner is its only owner. Another snapshot owner keeps the shared object live.
    pub fn validate_reclaimable_membership_rollup(
        &self,
        rollup: &crate::store_commit::MembershipRollupRef,
        owner: &SnapshotObjectOwner,
    ) -> Result<(), RemoteObjectRecordError> {
        self.validate()?;
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        if !matches!(
            &record.identity.domain,
            SharedLiveSetObjectDomain::StoreMembershipRollup { reference } if reference == rollup
        ) || record.identity.semantic_hash != rollup.rollup_hash
            || record.identity.object != rollup.object
            || !matches!(record.payloads, RemoteObjectPayloads::SpooledExternal)
        {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        let OwnedObjectState::UploadedVerified { ownership } = &record.state else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        if !ownership
            .activated
            .contains(&SharedObjectOwner::Snapshot(owner.clone()))
            || !ownership.pending.is_empty()
            || ownership.activated.len() != 1
        {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(())
    }

    fn activated_circle_bootstrap_image_ownership<'a>(
        &'a self,
        image: &crate::store_commit::SnapshotImageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<&'a SharedObjectOwnership, RemoteObjectRecordError> {
        self.validate()?;
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        let expected_owner = SharedObjectOwner::StoreCommit(activation.clone());
        if !matches!(
            &record.identity.domain,
            SharedLiveSetObjectDomain::CircleBootstrapImage { reference } if reference == image
        ) || record.identity.semantic_hash != image.image_hash
            || record.identity.object != image.object
            || !matches!(record.payloads, RemoteObjectPayloads::SpooledExternal)
        {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        let OwnedObjectState::UploadedVerified { ownership } = &record.state else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        if !ownership.activated.contains(&expected_owner) {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(ownership)
    }

    /// A stored blob is reclaimable when it is the exact activated blob the target
    /// names. Unlike a package or an image, a blob legitimately carries several
    /// activated owners — one per commit that bound it — so ownership count is not
    /// the eligibility question here; whether any live row or installable image
    /// still needs it is, and the reclaim verified that before reaching closure.
    pub fn validate_reclaimable_stored_blob(
        &self,
        stored: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), RemoteObjectRecordError> {
        self.validate()?;
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        let locator_bytes = stored.locator().to_bytes();
        if record.identity.domain != SharedLiveSetObjectDomain::StoredBlob
            || record.identity.semantic_hash != ObjectHash::digest(&locator_bytes)
            || record.identity.object != *stored.object()
            || record.payloads.carried_locator_bytes() != Some(locator_bytes.as_slice())
        {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        let OwnedObjectState::UploadedVerified { ownership } = &record.state else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        if !ownership.pending.is_empty() || ownership.activated.is_empty() {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(())
    }

    /// The activated Store commits that published this stored blob. A blob accretes
    /// one per commit whose package bindings named it, so a republished blob carries
    /// several.
    pub fn stored_blob_commit_owners(&self) -> Vec<StoreBatchCommitRef> {
        let Self::SharedLiveSet(record) = self else {
            return Vec::new();
        };
        if record.identity.domain != SharedLiveSetObjectDomain::StoredBlob {
            return Vec::new();
        }
        let OwnedObjectState::UploadedVerified { ownership } = &record.state else {
            return Vec::new();
        };
        ownership
            .activated
            .iter()
            .filter_map(|owner| match owner {
                SharedObjectOwner::StoreCommit(commit) => Some(commit.clone()),
                SharedObjectOwner::Snapshot(_) => None,
            })
            .collect()
    }

    pub fn snapshot_owners(&self) -> impl Iterator<Item = &SnapshotObjectOwner> {
        let owners = match self {
            Self::SharedLiveSet(record) => match &record.state {
                OwnedObjectState::UploadedVerified { ownership } => Some(&ownership.activated),
                OwnedObjectState::Prepared { .. } | OwnedObjectState::RetirementPending { .. } => {
                    None
                }
            },
            _ => None,
        };
        owners.into_iter().flat_map(|owners| {
            owners.iter().filter_map(|owner| match owner {
                SharedObjectOwner::Snapshot(owner) => Some(owner),
                SharedObjectOwner::StoreCommit(_) => None,
            })
        })
    }
}
