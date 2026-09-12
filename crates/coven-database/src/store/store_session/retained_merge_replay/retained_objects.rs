use super::*;
use crate::query_mapped_rows;

/// One retained materialization's claim on the objects its replay still needs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RetainedReplayOwner {
    pub commit: StoreBatchCommitRef,
    pub input_hash: ObjectHash,
}

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
    pub(crate) fn from_baseline(baseline: Option<&'a RetainedReplayBaseline>) -> Self {
        match baseline {
            Some(RetainedReplayBaseline {
                authority: RetainedReplayAuthority::InstalledSnapshot(snapshot),
                ..
            }) => Self::Snapshot {
                coverage: &snapshot.metadata.coverage,
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
            },
            Some(RetainedReplayBaseline {
                authority: RetainedReplayAuthority::Genesis(_),
                ..
            })
            | None => Self::Uncovered,
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
        .filter(|package| coverage.retains(&owner.commit, package))
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
        let removed = conn.execute(
            "DELETE FROM retained_replay_objects WHERE object_id = ?1 AND commit_ref = ?2 AND input_hash = ?3",
            rusqlite::params![
                object_id.to_string(),
                serde_json::to_string(&owner.commit)?,
                owner.input_hash.to_string()
            ],
        )?;
        if removed != 1 {
            return Err(DbError::Message(format!(
                "retiring replay ownership changed {removed} index rows for {object_id}"
            )));
        }
    }
    let commit = &owner.commit;
    let mut pinned = BTreeSet::new();
    for retained in input
        .packages
        .iter()
        .filter(|package| coverage.retains(commit, package))
    {
        let object_id = remote_object_id(retained.object());
        let remote = load_remote_object_on(conn, object_id)?;
        validate_retained_package_remote(&remote, retained, commit)?;
        index_retained_replay_owner_on(conn, object_id, owner)?;
        pinned.insert(object_id);
        for binding in retained.package().blob_bindings() {
            let stored = binding.blob();
            let object_id = remote_object_id(stored.object());
            if !pinned.insert(object_id) {
                continue;
            }
            let remote = load_remote_object_on(conn, object_id)?;
            validate_retained_blob_remote(&remote, stored, commit)?;
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
    let commit = &owner.commit;
    for retained in input
        .packages
        .iter()
        .filter(|package| coverage.retains(commit, package))
    {
        let remote = load_remote_object_on(conn, remote_object_id(retained.object()))?;
        validate_retained_package_remote(&remote, retained, commit)?;
        for binding in retained.package().blob_bindings() {
            let stored = binding.blob();
            let remote = load_remote_object_on(conn, remote_object_id(stored.object()))?;
            validate_retained_blob_remote(&remote, stored, commit)?;
        }
    }
    let expected = retained_merge_object_ids(input, owner, coverage);
    let actual = indexed_retained_merge_objects(conn, owner)?;
    if actual != expected {
        return Err(DbError::Message(format!(
            "retained Merge replay ownership differs from its exact object closure for {:?}: missing {:?}, extra {:?}",
            owner.commit,
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
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = &owner.commit.coord;
    let stream_id = stream_id.to_string();
    let sequence = Database::sequence_to_sqlite(&stream_id, *sequence)?;
    let commit_ref = serde_json::to_string(&owner.commit)
        .map_err(|error| DbError::context("serialize retained replay commit ref", error))?;
    let input_hash = owner.input_hash.to_string();
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

/// Drop every replay pin an exported or adopted image must not inherit.
pub(crate) fn clear_retained_replay_index_on(
    conn: &rusqlite::Transaction<'_>,
) -> Result<(), DbError> {
    conn.execute("DELETE FROM retained_replay_objects", [])
        .map_err(DbError::from)?;
    Ok(())
}
