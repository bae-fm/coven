use super::*;

pub(crate) struct StoreCommitVerifier<'a> {
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    verified_root: super::store_commit::StoreProtocolRoot,
    commits: BTreeMap<StoreBatchCommitRef, VerifiedStoreBatchCommit>,
}

impl<'a> StoreCommitVerifier<'a> {
    pub(crate) async fn new(
        storage: &'a dyn SyncStorage,
        root: &'a StoreRootRef,
    ) -> Result<Self, StoreObjectError> {
        let verified_root = load_store_protocol_root(storage, root).await?.value;
        Ok(Self {
            storage,
            root,
            verified_root,
            commits: BTreeMap::new(),
        })
    }

    pub(crate) async fn load_ref(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        if let Some(commit) = self.commits.get(reference) {
            return Ok(commit.clone());
        }
        let verified =
            load_verified_commit_at_root(self.storage, self.root, &self.verified_root, reference)
                .await?;
        self.commits.insert(reference.clone(), verified.clone());
        Ok(verified)
    }

    pub(crate) fn verified_root(&self) -> &super::store_commit::StoreProtocolRoot {
        &self.verified_root
    }

    pub(crate) fn remember(
        &mut self,
        commit: VerifiedStoreBatchCommit,
    ) -> Result<(), StoreProtocolError> {
        if commit.store_root_hash() != self.root.store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: self.root.store_root_hash,
                actual: commit.store_root_hash(),
            });
        }
        self.commits.insert(commit.reference().clone(), commit);
        Ok(())
    }
}

pub(crate) async fn load_commit_with_author(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &StoreBatchCommitRef,
) -> Result<(StoreBatchCommit, StoreDeviceRegistration), StoreObjectError> {
    let root_value = load_store_protocol_root(storage, root).await?.value;
    load_commit_with_author_at_root(storage, root, &root_value, reference).await
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CommitCoverageError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("exact Store ancestry is missing commit {commit_hash}")]
    MissingAncestry { commit_hash: ObjectHash },
}

pub(crate) async fn commit_position_covers(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    covering: &StoreBatchCommitRef,
    covered: &StoreBatchCommitRef,
) -> Result<bool, CommitCoverageError> {
    let same_stream = covering.coord.stream_id == covered.coord.stream_id;
    if !same_stream || covering.coord.sequence() < covered.coord.sequence() {
        return Ok(false);
    }
    let mut cursor = covering.clone();
    while cursor.coord.sequence() > covered.coord.sequence() {
        let (commit, _) = load_commit_with_author(storage, root, &cursor).await?;
        cursor =
            commit
                .order
                .predecessor()
                .cloned()
                .ok_or(CommitCoverageError::MissingAncestry {
                    commit_hash: cursor.commit_hash,
                })?;
    }
    Ok(cursor == *covered)
}

fn coverage_error(error: CommitCoverageError) -> StorePullError {
    match error {
        CommitCoverageError::Object(error) => StorePullError::Object(error),
        CommitCoverageError::MissingAncestry { commit_hash } => StorePullError::Database(format!(
            "exact Store ancestry is missing commit {commit_hash}"
        )),
    }
}

pub(crate) async fn history_cut_covers(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    cut: &StoreHistoryCut,
    covered: &StoreBatchCommitRef,
) -> Result<bool, StorePullError> {
    let covering = cut.0.get(&covered.coord.stream_id);
    match covering {
        Some(covering) => commit_position_covers(storage, root, covering, covered)
            .await
            .map_err(coverage_error),
        None => Ok(false),
    }
}

pub(crate) async fn load_provider_access_activation(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    access: &super::provider::ActivatedStoreMemberProviderAccessGrant,
    administrator: &StoreDeviceRegistration,
) -> Result<VerifiedStoreBatchCommit, StorePullError> {
    let grant = super::store_objects::load_provider_access_grant_ref_with_root(
        storage,
        root,
        history_verifier.verified_root(),
        &access.grant_ref,
        administrator,
    )
    .await?;
    if grant.value != access.grant {
        return Err(StorePullError::Database(
            "device provider approval embeds a different access grant than its exact reference"
                .to_string(),
        ));
    }
    let activation = history_verifier.load_ref(&access.activation).await?;
    if activation.value().provider_access_grants() != std::slice::from_ref(&access.grant_ref)
        || activation.value().author_registration != access.grant.administrator
        || activation.author() != administrator
    {
        return Err(StorePullError::Database(
            "device provider approval activation is not the administrator's exact sole access grant"
                .to_string(),
        ));
    }
    Ok(activation)
}

pub(crate) struct LoadedDeviceJoinAttemptEvidence {
    pub(crate) attempt: VerifiedObject<DeviceJoinAttempt>,
}

pub(crate) fn load_device_join_attempt_evidence_ref<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    reference: &'a super::store_commit::DeviceJoinAttemptRef,
    owner: &'a StoreDeviceRegistration,
) -> StorePullFuture<'a, LoadedDeviceJoinAttemptEvidence> {
    Box::pin(async move {
        let attempt =
            load_owner_signed_device_join_attempt_ref(storage, root, reference, owner).await?;
        let verified_root = load_store_protocol_root(storage, root).await?;
        if attempt.value.store_root != *root {
            return Err(StorePullError::Database(
                "device join attempt names another Store root".to_string(),
            ));
        }
        let offer = &attempt.value.provider_approval.request.offer;
        let administrator =
            load_registration_ref(storage, root, &offer.provider_admin.administrator)
                .await?
                .value;
        attempt
            .value
            .provider_approval
            .verify(&verified_root, owner, &administrator)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        Ok(LoadedDeviceJoinAttemptEvidence { attempt })
    })
}

pub(crate) fn load_commit_with_author_at_root<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    root_value: &'a super::store_commit::StoreProtocolRoot,
    reference: &'a StoreBatchCommitRef,
) -> super::store_objects::StoreObjectFuture<'a, (StoreBatchCommit, StoreDeviceRegistration)> {
    Box::pin(async move {
        let verified = load_verified_commit_at_root(storage, root, root_value, reference).await?;
        let (_, commit, author) = verified.into_parts();
        Ok((commit, author))
    })
}

pub(crate) fn load_verified_commit_at_root<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    root_value: &'a super::store_commit::StoreProtocolRoot,
    reference: &'a StoreBatchCommitRef,
) -> super::store_objects::StoreObjectFuture<'a, VerifiedStoreBatchCommit> {
    Box::pin(load_verified_commit_at_root_impl(
        storage, root, root_value, reference,
    ))
}

async fn load_verified_commit_at_root_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    reference: &StoreBatchCommitRef,
) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
    let semantic_prefix =
        super::store_commit::semantic_prefix_from_exact_object(&reference.object, ".json")
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: "Store candidate commit".to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let bytes = storage
        .read_protocol_object(&context, &reference.object, &semantic_prefix)
        .await
        .map_err(StoreObjectError::Storage)?;
    #[derive(serde::Deserialize)]
    struct StoreCommitAuthorProjection {
        author_registration: StoreDeviceRegistrationRef,
    }

    let parse_bytes = bytes.clone();
    let author_reference = run_blocking_object_verification(
        &semantic_prefix,
        &reference.object,
        Box::new(move || {
            serde_json::from_slice::<StoreCommitAuthorProjection>(&parse_bytes)
                .map(|projection| projection.author_registration)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
        }),
    )
    .await?;
    let author = load_registration_ref_with_root(storage, root, root_value, &author_reference)
        .await?
        .value;
    let expected_reference = reference.clone();
    let expected_author = author.clone();
    let store_root_hash = root.store_root_hash;
    let verify_bytes = bytes;
    let commit = run_blocking_object_verification(
        &semantic_prefix,
        &reference.object,
        Box::new(move || {
            VerifiedStoreBatchCommit::parse(
                &verify_bytes,
                store_root_hash,
                &expected_reference,
                &expected_author,
            )
        }),
    )
    .await?;
    Ok(commit)
}
