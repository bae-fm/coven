use super::*;
use crate::reclaim::{AudienceBlobBindingPackage, ReclaimAuthorizationRef, ReclaimTarget};

/// Exact package provenance retained while the remote package still exists.
/// Snapshot authentication carries this fact after its commit is retired.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedPackageActivation {
    pub package: AudienceBlobBindingPackage,
    pub activation: StoreBatchCommitRef,
    pub commit: StoreBatchCommit,
}

impl RetainedPackageActivation {
    pub fn matches_package(
        &self,
        package: &AudienceBlobBindingPackage,
        activation: &StoreBatchCommitRef,
    ) -> bool {
        &self.package == package && &self.activation == activation
    }

    fn validate(&self) -> Result<(), StoreProtocolError> {
        self.activation.verify_commit(&self.commit)?;
        let named = match &self.package {
            AudienceBlobBindingPackage::Store(package) => {
                self.commit.store_package() == Some(package)
            }
            AudienceBlobBindingPackage::Circle(package) => {
                self.commit.circle_packages().contains(package)
            }
        };
        if !named {
            return Err(StoreProtocolError::Malformed(
                "retained package differs from its exact activating commit".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedReclaimAuthorization {
    pub authorization: ReclaimAuthorizationRef,
    pub activation: StoreBatchCommitRef,
}

/// Exact artifacts of a superseded accepted snapshot awaiting physical retirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedStoreSnapshotOwnership {
    pub accepted: AcceptedStoreSnapshotRef,
    pub image: SnapshotImageRef,
    pub rollup: MembershipRollupRef,
}

impl RetainedStoreSnapshotOwnership {
    pub fn objects(&self) -> [&ExactObjectRef; 4] {
        [
            &self.image.object,
            &self.rollup.object,
            &self.accepted.snapshot.object,
            &self.accepted.publication.object,
        ]
    }
}

/// Reclamation's live objects and unfinished accepted authorizations.
/// Completions release their exact facts; ordinary covered commits are not
/// retained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedReclaimState {
    #[serde(with = "ordered_map_entries")]
    pub snapshots: BTreeMap<ObjectHash, RetainedStoreSnapshotOwnership>,
    #[serde(with = "ordered_map_entries")]
    pub publications: BTreeMap<ObjectHash, StorePublicationRef>,
    #[serde(with = "ordered_map_entries")]
    pub packages: BTreeMap<ObjectHash, RetainedPackageActivation>,
    #[serde(with = "ordered_map_entries")]
    pub authorizations: BTreeMap<ObjectHash, RetainedReclaimAuthorization>,
}

impl RetainedReclaimState {
    pub fn genesis() -> Self {
        Self {
            snapshots: BTreeMap::new(),
            publications: BTreeMap::new(),
            packages: BTreeMap::new(),
            authorizations: BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> Result<(), StoreProtocolError> {
        for (id, snapshot) in &self.snapshots {
            snapshot.accepted.publication.validate_slot()?;
            crate::objects::ProtocolObjectContext::signed_plaintext(
                snapshot.accepted.publication.store_root_hash,
                crate::objects::ProtocolObjectDomain::StoreSnapshotMeta,
            )
            .validate_reference(
                &snapshot.accepted.snapshot.object,
                &semantic_prefix_from_exact_object(&snapshot.accepted.snapshot.object, ".json")?,
            )?;
            snapshot
                .accepted
                .snapshot
                .validate_artifact_slots(&snapshot.image, &snapshot.rollup)?;
            if *id != snapshot.accepted.snapshot.snapshot_hash
                || snapshot.rollup.object == snapshot.accepted.snapshot.object
            {
                return Err(StoreProtocolError::Malformed(
                    "retained snapshot rollup has inconsistent exact ownership".to_string(),
                ));
            }
        }
        for (id, publication) in &self.publications {
            if *id != crate::remote_object::remote_object_id(&publication.object) {
                return Err(StoreProtocolError::Malformed(
                    "retired publication has inconsistent exact identity".into(),
                ));
            }
            publication.validate_slot()?;
        }
        for (id, package) in &self.packages {
            package.validate()?;
            if *id != crate::remote_object::remote_object_id(package.package.object())
                || package.package.object() == &package.activation.object
            {
                return Err(StoreProtocolError::Malformed(
                    "retained reclaim package has inconsistent exact provenance".to_string(),
                ));
            }
        }
        for (id, authorization) in &self.authorizations {
            if *id != authorization.authorization.authorization_hash
                || authorization.authorization.object == authorization.activation.object
            {
                return Err(StoreProtocolError::Malformed(
                    "retained reclaim authorization has inconsistent exact activation".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn validate_before(
        &self,
        successor: &StorePublicationRef,
    ) -> Result<(), StoreProtocolError> {
        self.validate()?;
        for publication in self.publications.values().chain(
            self.snapshots
                .values()
                .map(|snapshot| &snapshot.accepted.publication),
        ) {
            if publication.store_root_hash != successor.store_root_hash
                || publication.position >= successor.position
            {
                return Err(StoreProtocolError::Malformed(
                    "retired artifact is not before its accepted successor".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn include_previous_snapshot(
        &mut self,
        accepted: &AcceptedStoreSnapshotRef,
        metadata: &SnapshotMeta,
    ) -> Result<(), StoreProtocolError> {
        if metadata.snapshot_hash() != accepted.snapshot.snapshot_hash
            || metadata.publication_predecessor.next_position()? != accepted.publication.position
        {
            return Err(StoreProtocolError::Malformed(
                "retained snapshot ownership differs from its accepted boundary".to_string(),
            ));
        }
        self.snapshots = metadata.history_summary.reclaim.snapshots.clone();
        self.publications = metadata.history_summary.reclaim.publications.clone();
        self.snapshots.insert(
            accepted.snapshot.snapshot_hash,
            RetainedStoreSnapshotOwnership {
                accepted: accepted.clone(),
                image: metadata.image.clone(),
                rollup: metadata.membership_rollup.clone(),
            },
        );
        Ok(())
    }

    /// Fold the accepted interval as a set of additions and corresponding
    /// completions. Traversal order does not change which exact objects the
    /// interval retires.
    pub fn extend<'a>(
        &mut self,
        commits: impl IntoIterator<Item = (&'a StoreBatchCommitRef, &'a StoreBatchCommit)>,
    ) -> Result<(), StoreProtocolError> {
        let commits = commits.into_iter().collect::<Vec<_>>();
        for (reference, commit) in &commits {
            reference.verify_commit(commit)?;
            let packages = commit
                .store_package()
                .cloned()
                .map(AudienceBlobBindingPackage::Store)
                .into_iter()
                .chain(
                    commit
                        .circle_packages()
                        .iter()
                        .cloned()
                        .map(AudienceBlobBindingPackage::Circle),
                );
            for package in packages {
                let id = crate::remote_object::remote_object_id(package.object());
                let value = RetainedPackageActivation {
                    package,
                    activation: (*reference).clone(),
                    commit: (*commit).clone(),
                };
                if self
                    .packages
                    .get(&id)
                    .is_some_and(|existing| existing != &value)
                {
                    return Err(StoreProtocolError::Malformed(
                        "accepted packages disagree on exact activation".to_string(),
                    ));
                }
                self.packages.insert(id, value);
            }
            if let Some(authorization) = commit.reclaim_authorization() {
                let value = RetainedReclaimAuthorization {
                    authorization: authorization.clone(),
                    activation: (*reference).clone(),
                };
                if self
                    .authorizations
                    .get(&authorization.authorization_hash)
                    .is_some_and(|existing| existing != &value)
                {
                    return Err(StoreProtocolError::Malformed(
                        "reclaim authorization has conflicting accepted activations".to_string(),
                    ));
                }
                self.authorizations
                    .insert(authorization.authorization_hash, value);
            }
        }
        self.retire_completions(
            commits
                .into_iter()
                .filter_map(|(_, commit)| commit.reclaim_completion()),
        )
    }

    pub fn retire_completions<'a>(
        &mut self,
        completions: impl IntoIterator<Item = &'a crate::reclaim::ReclaimCompletion>,
    ) -> Result<(), StoreProtocolError> {
        for completion in completions {
            let target = completion.authorization.target();
            if matches!(
                target,
                ReclaimTarget::StorePackage(_) | ReclaimTarget::CirclePackage(_)
            ) {
                let id = crate::remote_object::remote_object_id(target.object());
                if let Some(package) = self.packages.get(&id) {
                    let matches = match target {
                        ReclaimTarget::StorePackage(target) => {
                            package.package
                                == AudienceBlobBindingPackage::Store(target.package.clone())
                                && package.activation == target.activation
                        }
                        ReclaimTarget::CirclePackage(target) => {
                            package.package
                                == AudienceBlobBindingPackage::Circle(target.package.clone())
                                && package.activation == target.activation
                        }
                        _ => unreachable!("matched package target"),
                    };
                    if !matches {
                        return Err(StoreProtocolError::Malformed(
                            "reclaim completion names another package activation".to_string(),
                        ));
                    }
                }
                self.packages.remove(&id);
            }
            if let Some(authorization) = self
                .authorizations
                .get(&completion.authorization.authorization_hash)
            {
                if authorization.authorization != completion.authorization {
                    return Err(StoreProtocolError::Malformed(
                        "reclaim completion names another exact authorization".to_string(),
                    ));
                }
            }
            self.authorizations
                .remove(&completion.authorization.authorization_hash);
        }
        self.validate()
    }
}
