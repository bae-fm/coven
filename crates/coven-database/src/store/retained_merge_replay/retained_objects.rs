use super::*;
use crate::query_mapped_rows;

impl StoreDatabase {
    pub fn canonical_retained_merge_packages(
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
            let Some(package) = by_audience.remove(&RemoteAudience::Circle(reference.circle_id))
            else {
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

    pub fn retained_merge_object_ids(
        input: &RetainedMergeMaterializationInput,
    ) -> BTreeSet<ObjectHash> {
        let mut object_ids = BTreeSet::new();
        for retained in &input.packages {
            object_ids.insert(remote_object_id(retained.object()));
            for binding in retained.package().blob_bindings() {
                object_ids.insert(remote_object_id(binding.blob().object()));
            }
        }
        object_ids
    }

    pub fn validate_retained_package_remote(
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

    pub fn validate_retained_blob_remote(
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

    pub fn pin_retained_merge_objects_on(
        conn: &rusqlite::Transaction<'_>,
        input: &RetainedMergeMaterializationInput,
        owner: &RetainedReplayOwner,
    ) -> Result<(), DbError> {
        let commit = owner.commit();
        let mut pinned = BTreeSet::new();
        for retained in &input.packages {
            let object_id = remote_object_id(retained.object());
            let mut remote = load_remote_object_on(conn, object_id)?;
            Self::validate_retained_package_remote(&remote, retained, commit)?;
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
                Self::validate_retained_blob_remote(&remote, stored, commit)?;
                remote
                    .merge_retained_replay_owner(owner.clone())
                    .map_err(|error| {
                        DbError::context(format!("pin retained blob {object_id} for replay"), error)
                    })?;
                update_remote_object_on(conn, object_id, &remote)?;
                index_retained_replay_owner_on(conn, object_id, owner)?;
            }
        }
        Ok(())
    }

    pub fn validate_retained_merge_pin_closure_on(
        conn: &Connection,
        input: &RetainedMergeMaterializationInput,
        owner: &RetainedReplayOwner,
    ) -> Result<(), DbError> {
        let commit = owner.commit();
        for retained in &input.packages {
            let remote = load_remote_object_on(conn, remote_object_id(retained.object()))?;
            Self::validate_retained_package_remote(&remote, retained, commit)?;
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
                Self::validate_retained_blob_remote(&remote, stored, commit)?;
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
        let expected = Self::retained_merge_object_ids(input);
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
        let mut rows = conn
            .prepare(
                "SELECT object_id FROM retained_replay_objects
                 WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3 AND input_hash = ?4
                 ORDER BY object_id",
            )
            .map_err(DbError::from)?;
        let actual = rows
            .query_map(
                rusqlite::params![stream_id, sequence, commit_ref, input_hash],
                |row| row.get::<_, String>(0),
            )
            .map_err(DbError::from)?
            .map(|row| {
                let object_id = row.map_err(DbError::from)?;
                object_id.parse().map_err(|error| {
                    DbError::context(format!("retained replay object id {object_id}"), error)
                })
            })
            .collect::<Result<BTreeSet<_>, DbError>>()?;
        if actual != expected {
            return Err(DbError::Message(
                "retained Merge replay ownership differs from its exact object closure".to_string(),
            ));
        }
        Ok(())
    }

    pub fn remove_retained_replay_ownership_from_snapshot_on(
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
}
