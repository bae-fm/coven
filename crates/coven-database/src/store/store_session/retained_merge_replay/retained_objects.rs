use super::*;
use crate::query_mapped_rows;

/// The Store objects a replay baseline still needs. Circle packages have their
/// own coverage and remain under their continuing replay owner.
pub(crate) enum RetainedReplayObjectCoverage<'a> {
    Uncovered,
    Snapshot {
        coverage: &'a CommitFrontier,
        pending_joins: BTreeSet<StoreBatchCommitRef>,
    },
}

impl<'a> RetainedReplayObjectCoverage<'a> {
    pub(crate) fn from_baseline(
        baseline: Option<&'a RetainedReplayBaseline>,
    ) -> Result<Self, DbError> {
        match baseline {
            Some(RetainedReplayBaseline {
                exact_cut,
                authority: RetainedReplayAuthority::InstalledSnapshot(snapshot),
                ..
            }) => {
                if exact_cut != &snapshot.metadata.coverage {
                    return Err(DbError::Message(
                        "replay object coverage differs from its installed snapshot".into(),
                    ));
                }
                Ok(Self::Snapshot {
                    coverage: exact_cut,
                    pending_joins: snapshot
                        .metadata
                        .history_summary
                        .pending_device_joins
                        .values()
                        .flat_map(|closure| {
                            closure
                                .commits
                                .iter()
                                .map(|commit| commit.reference.clone())
                        })
                        .collect(),
                })
            }
            Some(RetainedReplayBaseline {
                authority: RetainedReplayAuthority::Genesis(_),
                ..
            })
            | None => Ok(Self::Uncovered),
        }
    }

    fn retains(&self, reference: &StoreBatchCommitRef, package: &RetainedAudiencePackage) -> bool {
        match self {
            Self::Uncovered => true,
            Self::Snapshot {
                coverage,
                pending_joins,
            } => {
                package.package().audience().remote_audience() != RemoteAudience::Store
                    || !coverage.covers_commit(reference)
                    || pending_joins.contains(reference)
            }
        }
    }
}

pub(crate) fn canonical_retained_merge_packages(
    commit: &StoreBatchCommit,
    commit_ref: &StoreBatchCommitRef,
    packages: &[AudiencePackage],
) -> Result<Vec<RetainedAudiencePackage>, DbError> {
    let mut by_audience = BTreeMap::new();
    for package in packages {
        let audience = package.audience().remote_audience();
        if by_audience
            .insert(audience.clone(), package.clone())
            .is_some()
        {
            return Err(DbError::Message(format!(
                "retained Merge commit has duplicate {audience:?} packages"
            )));
        }
    }

    let mut ordered = Vec::new();
    if commit.store_package().is_some() {
        let package = by_audience.remove(&RemoteAudience::Store).ok_or_else(|| {
            DbError::Message("retained Merge commit is missing its Store package".to_string())
        })?;
        ordered.push(RetainedAudiencePackage::verify(
            commit, commit_ref, package,
        )?);
    }
    for reference in commit.circle_packages() {
        let Some(package) = by_audience.remove(&RemoteAudience::Circle(reference.circle_id)) else {
            continue;
        };
        ordered.push(RetainedAudiencePackage::verify(
            commit, commit_ref, package,
        )?);
    }
    if !by_audience.is_empty() {
        return Err(DbError::Message(
            "retained Merge input carries a package absent from its commit".to_string(),
        ));
    }
    Ok(ordered)
}

fn retained_merge_object_ids(
    input: &RetainedMergeMaterializationInput,
    owner: &RetainedReplayOwner,
    coverage: &RetainedReplayObjectCoverage<'_>,
) -> BTreeSet<ObjectHash> {
    let mut object_ids = BTreeSet::new();
    for retained in input
        .packages
        .iter()
        .filter(|package| coverage.retains(owner.commit(), package))
    {
        object_ids.insert(remote_object_id(retained.object()));
        for binding in retained.package().blob_bindings() {
            object_ids.insert(remote_object_id(binding.blob().object()));
        }
    }
    object_ids
}

fn validate_retained_package_remote(
    remote: &RemoteObjectRecord,
    retained: &RetainedAudiencePackage,
    owner: &StoreBatchCommitRef,
) -> Result<(), DbError> {
    let expected_domain = retained.domain();
    let expected_bytes = retained.package().to_bytes();
    let expected_owner =
        coven_protocol::remote_object::SharedObjectOwner::StoreCommit(owner.clone());
    if !matches!(
        remote,
        RemoteObjectRecord::SharedLiveSet(record)
            if record.identity.domain == expected_domain
                && record.identity.semantic_hash == ObjectHash::digest(&expected_bytes)
                && record.identity.object == *retained.object()
                && record.payloads.carried_locator_bytes().is_none()
                && matches!(
                    &record.state,
                    coven_protocol::remote_object::OwnedObjectState::UploadedVerified {
                        ownership
                    } if ownership.activated.contains(&expected_owner)
                )
    ) {
        return Err(DbError::Message(format!(
            "retained package {} differs from its exact activated remote object",
            remote_object_id(retained.object())
        )));
    }
    Ok(())
}

fn validate_retained_blob_remote(
    remote: &RemoteObjectRecord,
    stored: &StoredBlobRef,
    owner: &StoreBatchCommitRef,
) -> Result<(), DbError> {
    let locator_bytes = stored.locator().to_bytes();
    let expected_owner =
        coven_protocol::remote_object::SharedObjectOwner::StoreCommit(owner.clone());
    if !matches!(
        remote,
        RemoteObjectRecord::SharedLiveSet(record)
            if record.identity.domain == SharedLiveSetObjectDomain::StoredBlob
                && record.identity.semantic_hash == ObjectHash::digest(&locator_bytes)
                && record.identity.object == *stored.object()
                && record.payloads.carried_locator_bytes() == Some(locator_bytes.as_slice())
                && matches!(
                    &record.state,
                    coven_protocol::remote_object::OwnedObjectState::UploadedVerified {
                        ownership
                    } if ownership.activated.contains(&expected_owner)
                )
    ) {
        return Err(DbError::Message(format!(
            "retained blob {} differs from its exact activated remote object",
            remote_object_id(stored.object())
        )));
    }
    Ok(())
}

pub(crate) fn replace_retained_merge_object_ownership_on(
    conn: &rusqlite::Transaction<'_>,
    input: &RetainedMergeMaterializationInput,
    owner: &RetainedReplayOwner,
    coverage: &RetainedReplayObjectCoverage<'_>,
) -> Result<u64, DbError> {
    let expected = retained_merge_object_ids(input, owner, coverage);
    let all = retained_merge_object_ids(input, owner, &RetainedReplayObjectCoverage::Uncovered);
    let indexed = indexed_retained_merge_objects(conn, owner)?;
    if !indexed.is_subset(&all) {
        return Err(DbError::Message(
            "retained replay index claims an object absent from its exact input".into(),
        ));
    }
    let retired = indexed.difference(&expected).copied().collect::<Vec<_>>();
    for object_id in &retired {
        let RetainedReplayOwner::Commit { input_hash, .. } = owner;
        let mut remote = load_remote_object_on(conn, *object_id)?;
        if !remote
            .retained_replay_owners()
            .any(|actual| actual == owner)
        {
            return Err(DbError::Message(format!(
                "retained replay index names an absent owner for {object_id}"
            )));
        }
        remote
            .remove_retained_replay_owner(owner)
            .map_err(DbError::from)?;
        update_remote_object_on(conn, *object_id, &remote)?;
        let removed = conn.execute(
            "DELETE FROM retained_replay_objects WHERE object_id = ?1 AND commit_ref = ?2 AND input_hash = ?3",
            rusqlite::params![object_id.to_string(), serde_json::to_string(owner.commit())?, input_hash.to_string()],
        )?;
        if removed != 1 {
            return Err(DbError::Message(format!(
                "retiring replay ownership changed {removed} index rows for {object_id}"
            )));
        }
    }
    let commit = owner.commit();
    let mut pinned = BTreeSet::new();
    for retained in input
        .packages
        .iter()
        .filter(|package| coverage.retains(commit, package))
    {
        let object_id = remote_object_id(retained.object());
        let mut remote = load_remote_object_on(conn, object_id)?;
        validate_retained_package_remote(&remote, retained, commit)?;
        remote
            .merge_retained_replay_owner(owner.clone())
            .map_err(|error| {
                DbError::context(
                    format!("pin retained package {object_id} for replay"),
                    error,
                )
            })?;
        update_remote_object_on(conn, object_id, &remote)?;
        index_retained_replay_owner_on(conn, object_id, owner)?;
        pinned.insert(object_id);
        for binding in retained.package().blob_bindings() {
            let stored = binding.blob();
            let object_id = remote_object_id(stored.object());
            if !pinned.insert(object_id) {
                continue;
            }
            let mut remote = load_remote_object_on(conn, object_id)?;
            validate_retained_blob_remote(&remote, stored, commit)?;
            remote
                .merge_retained_replay_owner(owner.clone())
                .map_err(|error| {
                    DbError::context(format!("pin retained blob {object_id} for replay"), error)
                })?;
            update_remote_object_on(conn, object_id, &remote)?;
            index_retained_replay_owner_on(conn, object_id, owner)?;
        }
    }
    u64::try_from(retired.len())
        .map_err(|_| DbError::Message("released replay pin count exceeds u64".into()))
}

pub(crate) fn validate_retained_merge_pin_closure_on(
    conn: &Connection,
    input: &RetainedMergeMaterializationInput,
    owner: &RetainedReplayOwner,
    coverage: &RetainedReplayObjectCoverage<'_>,
) -> Result<(), DbError> {
    let commit = owner.commit();
    for retained in input
        .packages
        .iter()
        .filter(|package| coverage.retains(commit, package))
    {
        let remote = load_remote_object_on(conn, remote_object_id(retained.object()))?;
        validate_retained_package_remote(&remote, retained, commit)?;
        if !remote
            .retained_replay_owners()
            .any(|actual| actual == owner)
        {
            return Err(DbError::Message(format!(
                "retained package {} is missing its exact replay owner",
                remote_object_id(retained.object())
            )));
        }
        for binding in retained.package().blob_bindings() {
            let stored = binding.blob();
            let remote = load_remote_object_on(conn, remote_object_id(stored.object()))?;
            validate_retained_blob_remote(&remote, stored, commit)?;
            if !remote
                .retained_replay_owners()
                .any(|actual| actual == owner)
            {
                return Err(DbError::Message(format!(
                    "retained blob {} is missing its exact replay owner",
                    remote_object_id(stored.object())
                )));
            }
        }
    }
    let expected = retained_merge_object_ids(input, owner, coverage);
    let actual = indexed_retained_merge_objects(conn, owner)?;
    if actual != expected {
        return Err(DbError::Message(format!(
            "retained Merge replay ownership differs from its exact object closure for {:?}: missing {:?}, extra {:?}",
            owner.commit(),
            expected.difference(&actual).collect::<Vec<_>>(),
            actual.difference(&expected).collect::<Vec<_>>(),
        )));
    }
    Ok(())
}

fn indexed_retained_merge_objects(
    conn: &Connection,
    owner: &RetainedReplayOwner,
) -> Result<BTreeSet<ObjectHash>, DbError> {
    let RetainedReplayOwner::Commit { commit, input_hash } = owner;
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = &commit.coord;
    let stream_id = stream_id.to_string();
    let sequence = Database::sequence_to_sqlite(&stream_id, *sequence)?;
    let commit_ref = serde_json::to_string(commit)
        .map_err(|error| DbError::context("serialize retained replay commit ref", error))?;
    let input_hash = input_hash.to_string();
    query_mapped_rows(
        conn,
        "SELECT object_id FROM retained_replay_objects
                 WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3 AND input_hash = ?4
                 ORDER BY object_id",
        rusqlite::params![stream_id, sequence, commit_ref, input_hash],
        |row| row.get::<_, String>(0),
    )?
    .into_iter()
    .map(|object_id| {
        object_id.parse().map_err(|error| {
            DbError::context(format!("retained replay object id {object_id}"), error)
        })
    })
    .collect::<Result<BTreeSet<_>, DbError>>()
}

pub(crate) fn remove_retained_replay_ownership_from_snapshot_on(
    conn: &rusqlite::Transaction<'_>,
) -> Result<(), DbError> {
    let object_ids = query_mapped_rows(
        conn,
        "SELECT DISTINCT object_id FROM retained_replay_objects ORDER BY object_id",
        [],
        |row| row.get::<_, String>(0),
    )?;
    for encoded in object_ids {
        let object_id = encoded
            .parse()
            .map_err(|error| DbError::context("snapshot retained replay object id", error))?;
        let mut remote = load_remote_object_on(conn, object_id)?;
        remote
            .remove_all_retained_replay_owners()
            .map_err(|error| {
                DbError::context(
                    format!("remove snapshot retained replay owner from {object_id}"),
                    error,
                )
            })?;
        update_remote_object_on(conn, object_id, &remote)?;
    }
    conn.execute("DELETE FROM retained_replay_objects", [])
        .map_err(DbError::from)?;
    Ok(())
}
