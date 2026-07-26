use crate::database::blob_records::remote_audience_to_db;
use crate::database::store_reclaim_records::store_reclaim_journal_error;

use super::*;
use crate::sync::store::load_author_exclusion_activation_locator_on;

pub(crate) enum RemoteStoredRepresentationRef<'a> {
    Inline(&'a [u8]),
    Blob,
}

pub(crate) fn candidate_graph_exact_objects(
    commit: &StoreBatchCommit,
) -> Result<Vec<ExactObjectRef>, DbError> {
    crate::sync::remote_object::CandidateObjectGraph::from_commit(commit)
        .map(|graph| graph.exact_objects().cloned().collect())
        .map_err(|error| DbError::Message(format!("closed candidate object graph: {error}")))
}

pub(crate) fn validate_remote_object_on(
    conn: &Connection,
    object_id: ObjectHash,
    expected_object: &ExactObjectRef,
    expected_semantic_bytes: &[u8],
    expected_stored: RemoteStoredRepresentationRef<'_>,
) -> Result<(), DbError> {
    let remote = load_remote_object_on(conn, object_id)?;
    let stored = remote.bytes().stored();
    let stored_matches = match expected_stored {
        RemoteStoredRepresentationRef::Inline(expected) => stored.inline_bytes() == Some(expected),
        RemoteStoredRepresentationRef::Blob => matches!(
            stored,
            crate::sync::remote_object::RemoteStoredRepresentation::Blob { .. }
        ),
    };
    if remote.object() != expected_object
        || remote.bytes().canonical_semantic_bytes() != expected_semantic_bytes
        || !stored_matches
    {
        return Err(DbError::Message(format!(
            "prepared remote object {object_id} differs from its semantic index"
        )));
    }
    Ok(())
}

pub(crate) fn load_remote_object_on(
    conn: &Connection,
    object_id: ObjectHash,
) -> Result<RemoteObjectRecord, DbError> {
    let state: String = conn
        .query_row(
            "SELECT state FROM remote_objects WHERE object_id = ?1",
            [object_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => {
                DbError::Message(format!("prepared remote object {object_id} is absent"))
            }
            error => DbError::from(error),
        })?;
    let remote: RemoteObjectRecord = serde_json::from_str(&state).map_err(|error| {
        DbError::Message(format!(
            "prepared remote object {object_id} has invalid closed state: {error}"
        ))
    })?;
    remote.validate().map_err(|error| {
        DbError::Message(format!("prepared remote object {object_id}: {error}"))
    })?;
    validate_author_exclusion_proof_locators_on(conn, remote.nonactivation_proofs())?;
    let actual = remote_object_id(remote.object());
    if actual != object_id {
        return Err(DbError::Message(format!(
            "prepared remote object key is {object_id}, exact reference hashes to {actual}"
        )));
    }
    let indexed = indexed_retained_replay_owners_on(conn, object_id)?;
    let embedded = remote
        .retained_replay_owners()
        .cloned()
        .collect::<BTreeSet<_>>();
    if embedded != indexed {
        return Err(DbError::Message(format!(
            "prepared remote object {object_id} differs from its retained-replay ownership index"
        )));
    }
    Ok(remote)
}

pub(super) fn indexed_retained_replay_owners_on(
    conn: &Connection,
    object_id: ObjectHash,
) -> Result<BTreeSet<RetainedReplayOwner>, DbError> {
    let mut statement = conn
        .prepare(
            "SELECT device_id, seq, commit_ref, input_hash
             FROM retained_replay_objects WHERE object_id = ?1
             ORDER BY device_id, seq",
        )
        .map_err(DbError::from)?;
    let rows = statement
        .query_map([object_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(DbError::from)?;
    let mut owners = BTreeSet::new();
    for row in rows {
        let (device_id, sequence, encoded_commit, encoded_input_hash) =
            row.map_err(DbError::from)?;
        let commit: StoreBatchCommitRef =
            serde_json::from_str(&encoded_commit).map_err(|error| {
                DbError::Message(format!(
                    "retained replay object {object_id} commit ref: {error}"
                ))
            })?;
        let input_hash = encoded_input_hash.parse().map_err(|error| {
            DbError::Message(format!(
                "retained replay object {object_id} input hash: {error}"
            ))
        })?;
        let StoreCommitCoord {
            stream_id,
            sequence: commit_sequence,
        } = &commit.coord;
        let sequence = u64::try_from(sequence).map_err(|_| {
            DbError::Message(format!(
                "retained replay object {object_id} has an invalid sequence"
            ))
        })?;
        if stream_id.to_string() != device_id || *commit_sequence != sequence {
            return Err(DbError::Message(format!(
                "retained replay object {object_id} index differs from its commit coordinate"
            )));
        }
        if !owners.insert(RetainedReplayOwner::Commit { commit, input_hash }) {
            return Err(DbError::Message(format!(
                "retained replay object {object_id} repeats an owner"
            )));
        }
    }
    Ok(owners)
}

pub(crate) fn index_retained_replay_owner_on(
    conn: &rusqlite::Transaction<'_>,
    object_id: ObjectHash,
    owner: &RetainedReplayOwner,
) -> Result<(), DbError> {
    let RetainedReplayOwner::Commit { commit, input_hash } = owner;
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = &commit.coord;
    let device_id = stream_id.to_string();
    let sequence = Database::sequence_to_sqlite(&device_id, *sequence)?;
    let commit_ref = serde_json::to_string(commit).map_err(|error| {
        DbError::Message(format!("serialize retained replay commit ref: {error}"))
    })?;
    let input_hash = input_hash.to_string();
    conn.execute(
        "INSERT INTO retained_replay_objects
         (device_id, seq, commit_ref, input_hash, object_id)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(device_id, seq, object_id) DO NOTHING",
        rusqlite::params![
            &device_id,
            sequence,
            &commit_ref,
            &input_hash,
            object_id.to_string()
        ],
    )
    .map_err(DbError::from)?;
    let stored: (String, String) = conn
        .query_row(
            "SELECT commit_ref, input_hash FROM retained_replay_objects
             WHERE device_id = ?1 AND seq = ?2 AND object_id = ?3",
            rusqlite::params![device_id, sequence, object_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(DbError::from)?;
    if stored != (commit_ref, input_hash) {
        return Err(DbError::Message(format!(
            "retained replay object {object_id} already has different exact ownership"
        )));
    }
    Ok(())
}

pub(crate) fn load_protocol_inert_object_on(
    conn: &Connection,
    object_id: ObjectHash,
) -> Result<crate::sync::remote_object::ProtocolInertObject, DbError> {
    let state: String = conn
        .query_row(
            "SELECT state FROM protocol_inert_objects WHERE object_id = ?1",
            [object_id.to_string()],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    let inert: crate::sync::remote_object::ProtocolInertObject = serde_json::from_str(&state)
        .map_err(|error| {
            DbError::Message(format!(
                "protocol-inert object {object_id} has invalid closed state: {error}"
            ))
        })?;
    inert
        .validate()
        .map_err(|error| DbError::Message(format!("protocol-inert object {object_id}: {error}")))?;
    validate_author_exclusion_proof_locators_on(conn, inert.nonactivation_proofs())?;
    if inert.object_id() != object_id {
        return Err(DbError::Message(format!(
            "protocol-inert object key is {object_id}, exact reference hashes to {}",
            inert.object_id()
        )));
    }
    Ok(inert)
}

pub(super) fn validate_author_exclusion_proof_locators_on(
    conn: &Connection,
    proofs: Vec<&crate::sync::remote_object::CandidateNonactivationProof>,
) -> Result<(), DbError> {
    for proof in proofs {
        let crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion {
            exclusion,
            accepted_cut,
            activation_head,
        } = proof
        else {
            continue;
        };
        let locator = load_author_exclusion_activation_locator_on(conn, exclusion)?;
        if locator.accepted_cut() != accepted_cut
            || locator.activation_head() != activation_head
            || locator.exclusion() != exclusion
        {
            return Err(DbError::Message(
                "author-exclusion candidate proof differs from its immutable activation locator"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

pub(super) fn load_reclaimed_store_package_on(
    conn: &Connection,
    object_id: ObjectHash,
) -> Result<Option<ReclaimedStorePackage>, DbError> {
    let stored: Option<(String, String)> = conn
        .query_row(
            "SELECT authorization_hash, state FROM reclaimed_store_packages WHERE object_id = ?1",
            [object_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(DbError::from)?;
    let Some((authorization_hash, state)) = stored else {
        return Ok(None);
    };
    let authorization_hash = authorization_hash.parse::<ObjectHash>().map_err(|error| {
        DbError::Message(format!(
            "reclaimed Store package {object_id} has invalid authorization hash: {error}"
        ))
    })?;
    let reclaimed: ReclaimedStorePackage = serde_json::from_str(&state).map_err(|error| {
        DbError::Message(format!(
            "reclaimed Store package {object_id} has invalid closed state: {error}"
        ))
    })?;
    reclaimed.validate().map_err(store_reclaim_journal_error)?;
    if reclaimed.object_id() != object_id
        || reclaimed.authorization().authorization_hash != authorization_hash
    {
        return Err(DbError::Message(format!(
            "reclaimed Store package {object_id} differs from its indexed identity"
        )));
    }
    Ok(Some(reclaimed))
}

pub(crate) fn record_reclaimed_store_package_on(
    conn: &Connection,
    reclaimed: &ReclaimedStorePackage,
) -> Result<(), DbError> {
    reclaimed.validate().map_err(store_reclaim_journal_error)?;
    let object_id = reclaimed.object_id();
    if let Some(existing) = load_reclaimed_store_package_on(conn, object_id)? {
        if existing == *reclaimed {
            return Ok(());
        }
        if !matches!(
            (&existing, reclaimed),
            (
                ReclaimedStorePackage::AbsentVerified {
                    authorization: existing_authorization,
                    authorization_activation: existing_activation,
                },
                ReclaimedStorePackage::Receipted {
                    authorization,
                    authorization_activation,
                    ..
                }
            ) if existing_authorization == authorization
                && existing_activation == authorization_activation
        ) {
            return Err(DbError::Message(format!(
                "reclaimed Store package {object_id} has conflicting closed authority"
            )));
        }
        let state = serde_json::to_string(reclaimed).map_err(|error| {
            DbError::Message(format!("serialize reclaimed Store package: {error}"))
        })?;
        let updated = conn
            .execute(
                "UPDATE reclaimed_store_packages SET state = ?2 WHERE object_id = ?1",
                (object_id.to_string(), state),
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(format!(
                "reclaimed Store package {object_id} disappeared during receipt closure"
            )));
        }
        return Ok(());
    }

    let remote_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
            [object_id.to_string()],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if remote_exists {
        let remote = load_remote_object_on(conn, object_id)?;
        match reclaimed.authorization().target() {
            crate::sync::store::ReclaimTarget::StorePackage(target) => {
                remote.validate_reclaimable_store_package(&target.package, &target.activation)
            }
            crate::sync::store::ReclaimTarget::CirclePackage(target) => {
                remote.validate_reclaimable_circle_package(&target.package, &target.activation)
            }
            crate::sync::store::ReclaimTarget::CircleBootstrapImage(target) => remote
                .validate_reclaimable_circle_bootstrap_image(
                    &target.coverage.bootstrap.image,
                    &target.coverage.activation_commit,
                ),
            crate::sync::store::ReclaimTarget::CircleSnapshotImage(target) => {
                let root = required_store_root_authority_on(conn)?;
                let owner = target
                    .snapshot_owner(root.store_root_hash)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                remote.validate_reclaimable_snapshot_image(&target.image, &owner)
            }
            crate::sync::store::ReclaimTarget::AudienceBlob(target) => {
                remote.validate_reclaimable_stored_blob(&target.blob)
            }
        }
        .map_err(|error| {
            DbError::Message(format!("close reclaimed package {object_id}: {error}"))
        })?;
        // A stored blob is referenced by a chain: row bindings name its locator row,
        // which names its remote object. All three leave in this transaction or none
        // does. The bindings that remain here are stale by construction — the reclaim
        // verified no live row resolves to this blob — and they are what the foreign
        // keys would otherwise hold the locator row against.
        conn.execute(
            "DELETE FROM row_blob_locators WHERE remote_object_id = ?1",
            [object_id.to_string()],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "DELETE FROM blob_locators WHERE remote_object_id = ?1",
            [object_id.to_string()],
        )
        .map_err(DbError::from)?;
        let deleted = conn
            .execute(
                "DELETE FROM remote_objects WHERE object_id = ?1",
                [object_id.to_string()],
            )
            .map_err(DbError::from)?;
        if deleted != 1 {
            return Err(DbError::Message(format!(
                "Store package {object_id} disappeared during reclaim closure"
            )));
        }
    }
    let inert_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM protocol_inert_objects WHERE object_id = ?1)",
            [object_id.to_string()],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if inert_exists {
        return Err(DbError::Message(format!(
            "reclaimed Store package {object_id} is protocol-inert"
        )));
    }
    let state = serde_json::to_string(reclaimed)
        .map_err(|error| DbError::Message(format!("serialize reclaimed Store package: {error}")))?;
    let inserted = conn
        .execute(
            "INSERT INTO reclaimed_store_packages (object_id, authorization_hash, state) VALUES (?1, ?2, ?3)",
            (
                object_id.to_string(),
                reclaimed.authorization().authorization_hash.to_string(),
                state,
            ),
        )
        .map_err(DbError::from)?;
    if inserted != 1 {
        return Err(DbError::Message(format!(
            "reclaimed Store package {object_id} was not inserted"
        )));
    }
    Ok(())
}

pub(crate) fn persist_exact_remote_object_on(
    conn: &Connection,
    remote: &RemoteObjectRecord,
    domain: &str,
) -> Result<(), DbError> {
    remote
        .validate()
        .map_err(|error| DbError::Message(format!("prepared {domain}: {error}")))?;
    let object_id = remote.object_id();
    ensure_remote_object_is_writable_on(conn, object_id, domain)?;
    let existing = conn
        .query_row(
            "SELECT state FROM remote_objects WHERE object_id = ?1",
            [object_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?;
    if let Some(existing) = existing {
        let existing: RemoteObjectRecord = serde_json::from_str(&existing).map_err(|error| {
            DbError::Message(format!(
                "prepared {domain} {object_id} has invalid closed state: {error}"
            ))
        })?;
        if existing != *remote {
            return Err(DbError::Message(format!(
                "prepared {domain} {object_id} already has different closed state"
            )));
        }
        return Ok(());
    }
    let state = serde_json::to_string(remote)
        .map_err(|error| DbError::Message(format!("serialize prepared {domain}: {error}")))?;
    conn.execute(
        "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
        (object_id.to_string(), state),
    )
    .map_err(DbError::from)?;
    Ok(())
}

fn ensure_remote_object_is_writable_on(
    conn: &Connection,
    object_id: ObjectHash,
    domain: &str,
) -> Result<(), DbError> {
    if load_reclaimed_store_package_on(conn, object_id)?.is_some() {
        return Err(DbError::Message(format!(
            "prepared {domain} {object_id} is a reclaimed Store package"
        )));
    }
    let inert_exists: bool = conn
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM protocol_inert_objects WHERE object_id = ?1
             )",
            [object_id.to_string()],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if inert_exists {
        return Err(DbError::Message(format!(
            "prepared {domain} {object_id} is already protocol-inert"
        )));
    }
    Ok(())
}

pub(crate) fn persist_prepared_remote_object_on(
    conn: &Connection,
    remote: &RemoteObjectRecord,
    owner: &StoreBatchCommitRef,
    domain: &str,
) -> Result<(), DbError> {
    remote
        .validate()
        .map_err(|error| DbError::Message(format!("prepared {domain}: {error}")))?;
    let object_id = remote.object_id();
    ensure_remote_object_is_writable_on(conn, object_id, domain)?;
    let exists = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
            [object_id.to_string()],
            |row| row.get::<_, bool>(0),
        )
        .map_err(DbError::from)?;
    if !exists {
        return persist_exact_remote_object_on(conn, remote, domain);
    }
    let existing = load_remote_object_on(conn, object_id)?;
    let merged = merge_prepared_remote_object(existing, remote, owner)?;
    update_remote_object_on(conn, object_id, &merged)
}

pub(crate) fn update_remote_object_on(
    conn: &Connection,
    object_id: ObjectHash,
    remote: &RemoteObjectRecord,
) -> Result<(), DbError> {
    remote
        .validate()
        .map_err(|error| DbError::Message(format!("remote object {object_id}: {error}")))?;
    if remote.object_id() != object_id {
        return Err(DbError::Message(format!(
            "remote object {object_id} changed its exact identity"
        )));
    }
    let state = serde_json::to_string(remote)
        .map_err(|error| DbError::Message(format!("serialize remote object: {error}")))?;
    let updated = conn
        .execute(
            "UPDATE remote_objects SET state = ?2 WHERE object_id = ?1",
            (object_id.to_string(), state),
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(format!(
            "remote object {object_id} disappeared during state transition"
        )));
    }
    Ok(())
}

pub(crate) fn begin_remote_candidate_nonactivation_on(
    conn: &rusqlite::Transaction<'_>,
    object_id: ObjectHash,
    nonactivation: crate::sync::remote_object::CandidateNonactivation,
) -> Result<Option<ExactObjectRef>, DbError> {
    let mut remote = load_remote_object_on(conn, object_id)?;
    let inert = remote
        .begin_candidate_nonactivation(nonactivation)
        .map_err(|error| {
            DbError::Message(format!(
                "record candidate nonactivation for {object_id}: {error}"
            ))
        })?;
    finish_remote_candidate_nonactivation_on(conn, object_id, remote, inert)
}

pub(crate) fn begin_remote_candidate_nonactivation_with_verified_head_on(
    conn: &rusqlite::Transaction<'_>,
    object_id: ObjectHash,
    nonactivation: crate::sync::remote_object::CandidateNonactivation,
    head_nonactivation: &crate::sync::remote_object::VerifiedCandidateHeadNonactivation,
) -> Result<Option<ExactObjectRef>, DbError> {
    let mut remote = load_remote_object_on(conn, object_id)?;
    let inert = remote
        .begin_candidate_nonactivation_with_verified_head_nonactivation(
            nonactivation,
            head_nonactivation,
        )
        .map_err(|error| {
            DbError::Message(format!(
                "record candidate nonactivation for {object_id}: {error}"
            ))
        })?;
    finish_remote_candidate_nonactivation_on(conn, object_id, remote, inert)
}

pub(crate) fn finish_remote_candidate_nonactivation_on(
    conn: &rusqlite::Transaction<'_>,
    object_id: ObjectHash,
    remote: RemoteObjectRecord,
    inert: Option<crate::sync::remote_object::ProtocolInertObject>,
) -> Result<Option<ExactObjectRef>, DbError> {
    let Some(inert) = inert else {
        let cleanup = remote.cleanup_target().cloned();
        update_remote_object_on(conn, object_id, &remote)?;
        return Ok(cleanup);
    };
    if inert.object_id() != object_id {
        return Err(DbError::Message(format!(
            "protocol-inert object {object_id} changed its exact identity"
        )));
    }
    inert
        .validate()
        .map_err(|error| DbError::Message(format!("protocol-inert object {object_id}: {error}")))?;
    let encoded = serde_json::to_string(&inert)
        .map_err(|error| DbError::Message(format!("serialize protocol-inert object: {error}")))?;
    let removed = conn
        .execute(
            "DELETE FROM remote_objects WHERE object_id = ?1",
            [object_id.to_string()],
        )
        .map_err(DbError::from)?;
    if removed != 1 {
        return Err(DbError::Message(format!(
            "remote object {object_id} disappeared during protocol-inert transition"
        )));
    }
    let inserted = conn
        .execute(
            "INSERT INTO protocol_inert_objects (object_id, state) VALUES (?1, ?2)",
            (object_id.to_string(), encoded),
        )
        .map_err(DbError::from)?;
    if inserted != 1 {
        return Err(DbError::Message(format!(
            "protocol-inert object {object_id} was not inserted"
        )));
    }
    Ok(None)
}

pub(crate) fn replace_prepared_merge_head_remote_on(
    conn: &Connection,
    current: &ExactObjectRef,
    winner: &StoreDeviceHead,
    winner_prepared: &PreparedExactObject,
    candidate: &StoreBatchCommitRef,
) -> Result<(), DbError> {
    if winner_prepared.reference().slot() != current.slot()
        || winner_prepared.reference() == current
        || winner.commit != *candidate
    {
        return Err(DbError::Message(
            "alternate Merge head does not replace the prepared activation slot".to_string(),
        ));
    }
    let old_object_id = remote_object_id(current);
    let old_remote = load_remote_object_on(conn, old_object_id)?;
    if !matches!(
        &old_remote,
        RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.identity.domain,
                crate::sync::remote_object::RetainedAuthorityObjectDomain::DeviceHead { .. }
            ) && matches!(
                &record.state,
                crate::sync::remote_object::RetainedAuthorityObjectState::Prepared { ownership }
                    if ownership.pending == BTreeSet::from([candidate.clone()])
            )
    ) {
        return Err(DbError::Message(
            "prepared Merge head lost its candidate ownership".to_string(),
        ));
    }
    let removed = conn
        .execute(
            "DELETE FROM remote_objects WHERE object_id = ?1",
            [old_object_id.to_string()],
        )
        .map_err(DbError::from)?;
    if removed != 1 {
        return Err(DbError::Message(
            "prepared Merge head disappeared during replacement".to_string(),
        ));
    }
    let winner_ref = crate::sync::store_commit::StoreDeviceHeadRef {
        head_hash: winner.head_hash(),
        object: winner_prepared.reference().clone(),
    };
    let mut winner_remote = RemoteObjectRecord::candidate_activated_store_head(
        winner_ref,
        winner.to_bytes(),
        winner_prepared.stored_bytes().to_vec(),
        candidate.clone(),
    )
    .map_err(|error| DbError::Message(format!("alternate Merge head: {error}")))?;
    winner_remote.mark_uploaded_verified().map_err(|error| {
        DbError::Message(format!("mark alternate Merge head uploaded: {error}"))
    })?;
    persist_exact_remote_object_on(conn, &winner_remote, "alternate Merge head")
}

pub(crate) fn mark_remote_object_uploaded_on(
    conn: &Connection,
    expected: RemoteObjectRecord,
) -> Result<RemoteObjectRecord, DbError> {
    let object_id = expected.object_id();
    let current = load_remote_object_on(conn, object_id)?;
    if let (
        RemoteObjectRecord::SharedLiveSet(current_record),
        RemoteObjectRecord::CandidateExclusive(expected_record),
    ) = (&current, &expected)
    {
        let expected_owner = match &expected_record.state {
            crate::sync::remote_object::CandidateObjectState::Prepared { ownership }
            | crate::sync::remote_object::CandidateObjectState::UploadedVerified { ownership } => {
                ownership.pending.iter().next()
            }
            crate::sync::remote_object::CandidateObjectState::CleanupPending { .. }
            | crate::sync::remote_object::CandidateObjectState::AbsentVerified { .. } => None,
        };
        if expected_record.identity.domain.shared_destination()
            == Some(current_record.identity.domain.clone())
            && expected_record.identity.semantic_hash == current_record.identity.semantic_hash
            && expected_record.identity.object == current_record.identity.object
            && expected_record.bytes == current_record.bytes
            && expected_owner.is_some_and(|owner| {
                matches!(
                    &current_record.state,
                    crate::sync::remote_object::OwnedObjectState::UploadedVerified { ownership }
                        if ownership.pending.contains(owner)
                            || ownership.activated.contains(
                                &crate::sync::remote_object::SharedObjectOwner::StoreCommit(
                                    owner.clone(),
                                ),
                            )
                )
            })
        {
            return Ok(current);
        }
    }
    if let (
        RemoteObjectRecord::RetainedAuthority(current_record),
        RemoteObjectRecord::CandidateExclusive(expected_record),
    ) = (&current, &expected)
    {
        let expected_owner = match &expected_record.state {
            crate::sync::remote_object::CandidateObjectState::Prepared { ownership }
            | crate::sync::remote_object::CandidateObjectState::UploadedVerified { ownership } => {
                ownership.pending.iter().next()
            }
            crate::sync::remote_object::CandidateObjectState::CleanupPending { .. }
            | crate::sync::remote_object::CandidateObjectState::AbsentVerified { .. } => None,
        };
        if expected_record.identity.domain.retained_destination()
            == Some(current_record.identity.domain.clone())
            && expected_record.identity.semantic_hash == current_record.identity.semantic_hash
            && expected_record.identity.object == current_record.identity.object
            && expected_record.bytes == current_record.bytes
            && expected_owner.is_some_and(|owner| {
                matches!(
                    &current_record.state,
                    crate::sync::remote_object::RetainedAuthorityObjectState::UploadedVerified {
                        ownership
                    } if ownership.pending.contains(owner) || ownership.activated.contains(owner)
                )
            })
        {
            return Ok(current);
        }
    }
    let mut uploaded = expected.clone();
    uploaded.mark_uploaded_verified().map_err(|error| {
        DbError::Message(format!("mark remote object {object_id} uploaded: {error}"))
    })?;
    if current == uploaded {
        return Ok(current);
    }
    if current != expected {
        return Err(DbError::Message(format!(
            "remote object {object_id} changed before upload completion"
        )));
    }
    let expected_json = serde_json::to_string(&expected)
        .map_err(|error| DbError::Message(format!("serialize expected remote object: {error}")))?;
    let uploaded_json = serde_json::to_string(&uploaded)
        .map_err(|error| DbError::Message(format!("serialize uploaded remote object: {error}")))?;
    let updated = conn
        .execute(
            "UPDATE remote_objects SET state = ?3
             WHERE object_id = ?1 AND state = ?2",
            (object_id.to_string(), expected_json, uploaded_json),
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(format!(
            "remote object {object_id} lost upload ownership"
        )));
    }
    Ok(uploaded)
}

pub(crate) fn mark_reusable_retained_authority_uploaded_on(
    conn: &Connection,
    expected: RemoteObjectRecord,
) -> Result<RemoteObjectRecord, DbError> {
    let object_id = expected.object_id();
    let RemoteObjectRecord::RetainedAuthority(expected_record) = &expected else {
        return Err(DbError::Message(format!(
            "reusable remote object {object_id} is not retained authority"
        )));
    };
    let crate::sync::remote_object::RetainedAuthorityObjectState::Prepared {
        ownership: expected_ownership,
    } = &expected_record.state
    else {
        return Err(DbError::Message(format!(
            "reusable retained authority {object_id} is not prepared"
        )));
    };
    if expected_ownership.pending.len() != 1 || !expected_ownership.nonactivated.is_empty() {
        return Err(DbError::Message(format!(
            "reusable retained authority {object_id} has ambiguous expected ownership"
        )));
    }
    let candidate = expected_ownership
        .pending
        .iter()
        .next()
        .expect("validated one expected candidate");
    let mut current = load_remote_object_on(conn, object_id)?;
    let RemoteObjectRecord::RetainedAuthority(current_record) = &current else {
        return Err(DbError::Message(format!(
            "reusable retained authority {object_id} changed domain"
        )));
    };
    if current_record.identity != expected_record.identity
        || current_record.bytes != expected_record.bytes
    {
        return Err(DbError::Message(format!(
            "reusable retained authority {object_id} changed exact identity or bytes"
        )));
    }
    let owns_candidate = match &current_record.state {
        crate::sync::remote_object::RetainedAuthorityObjectState::Prepared { ownership } => {
            ownership.pending.contains(candidate)
        }
        crate::sync::remote_object::RetainedAuthorityObjectState::UploadedVerified {
            ownership,
        } => ownership.pending.contains(candidate) || ownership.activated.contains(candidate),
        crate::sync::remote_object::RetainedAuthorityObjectState::CleanupPending { .. }
        | crate::sync::remote_object::RetainedAuthorityObjectState::AbsentVerified { .. }
        | crate::sync::remote_object::RetainedAuthorityObjectState::UncreatedVerified { .. } => {
            false
        }
    };
    if !owns_candidate {
        return Err(DbError::Message(format!(
            "reusable retained authority {object_id} does not belong to its upload candidate"
        )));
    }
    let before = current.clone();
    current.mark_uploaded_verified().map_err(|error| {
        DbError::Message(format!(
            "mark reusable retained authority {object_id} uploaded: {error}"
        ))
    })?;
    if current != before {
        update_remote_object_on(conn, object_id, &current)?;
    }
    Ok(current)
}

pub(crate) fn merge_prepared_remote_object(
    existing: RemoteObjectRecord,
    proposed: &RemoteObjectRecord,
    owner: &StoreBatchCommitRef,
) -> Result<RemoteObjectRecord, DbError> {
    use crate::sync::remote_object::{OwnedObjectState, SharedLiveSetObjectDomain};

    if &existing == proposed {
        return Ok(existing);
    }
    if let (
        RemoteObjectRecord::SharedLiveSet(existing_record),
        RemoteObjectRecord::CandidateExclusive(proposed_record),
    ) = (&existing, proposed)
    {
        let proposed_owner = match &proposed_record.state {
            crate::sync::remote_object::CandidateObjectState::Prepared { ownership }
            | crate::sync::remote_object::CandidateObjectState::UploadedVerified { ownership } => {
                ownership.pending.contains(owner)
            }
            crate::sync::remote_object::CandidateObjectState::CleanupPending { .. }
            | crate::sync::remote_object::CandidateObjectState::AbsentVerified { .. } => false,
        };
        if proposed_record.identity.domain.shared_destination()
            != Some(existing_record.identity.domain.clone())
            || proposed_record.identity.semantic_hash != existing_record.identity.semantic_hash
            || proposed_record.identity.object != existing_record.identity.object
            || proposed_record.bytes != existing_record.bytes
            || !proposed_owner
        {
            return Err(DbError::Message(format!(
                "shared candidate object {} already has different identity, bytes, or ownership",
                proposed.object_id()
            )));
        }
        let mut merged = existing.clone();
        let RemoteObjectRecord::SharedLiveSet(record) = &mut merged else {
            unreachable!("matched shared live-set object")
        };
        match &mut record.state {
            OwnedObjectState::Prepared { ownership } => {
                ownership.pending.insert(owner.clone());
            }
            OwnedObjectState::UploadedVerified { ownership } => {
                ownership.pending.insert(owner.clone());
            }
            OwnedObjectState::RetirementPending { former_candidates } => {
                record.state = OwnedObjectState::UploadedVerified {
                    ownership: crate::sync::remote_object::SharedObjectOwnership {
                        pending: BTreeSet::from([owner.clone()]),
                        activated: BTreeSet::new(),
                        nonactivated: former_candidates.clone(),
                    },
                };
            }
        }
        merged.validate().map_err(|error| {
            DbError::Message(format!(
                "merge shared candidate object {}: {error}",
                proposed.object_id()
            ))
        })?;
        return Ok(merged);
    }
    if let (
        RemoteObjectRecord::RetainedAuthority(existing_record),
        RemoteObjectRecord::CandidateExclusive(proposed_record),
    ) = (&existing, proposed)
    {
        if proposed_record.identity.domain.retained_destination()
            != Some(existing_record.identity.domain.clone())
            || proposed_record.identity.semantic_hash != existing_record.identity.semantic_hash
            || proposed_record.identity.object != existing_record.identity.object
            || proposed_record.bytes != existing_record.bytes
        {
            return Err(DbError::Message(format!(
                "retained candidate object {} already has different identity or bytes",
                proposed.object_id()
            )));
        }
        let proposed_owner = match &proposed_record.state {
            crate::sync::remote_object::CandidateObjectState::Prepared { ownership }
            | crate::sync::remote_object::CandidateObjectState::UploadedVerified { ownership } => {
                ownership.pending.contains(owner)
            }
            crate::sync::remote_object::CandidateObjectState::CleanupPending { .. }
            | crate::sync::remote_object::CandidateObjectState::AbsentVerified { .. } => false,
        };
        if !proposed_owner {
            return Err(DbError::Message(format!(
                "retained candidate object {} does not name its preparing commit",
                proposed.object_id()
            )));
        }
        let mut merged = existing.clone();
        merged
            .add_retained_authority_candidate(owner.clone())
            .map_err(|error| {
                DbError::Message(format!(
                    "merge retained candidate object {}: {error}",
                    proposed.object_id()
                ))
            })?;
        return Ok(merged);
    }
    let (
        RemoteObjectRecord::SharedLiveSet(mut existing),
        RemoteObjectRecord::SharedLiveSet(proposed),
    ) = (existing, proposed)
    else {
        return Err(DbError::Message(format!(
            "remote object {} already has different closed state",
            proposed.object_id()
        )));
    };
    if existing.identity.domain != SharedLiveSetObjectDomain::StoredBlob
        || proposed.identity.domain != SharedLiveSetObjectDomain::StoredBlob
        || existing.identity != proposed.identity
        || existing.bytes != proposed.bytes
    {
        return Err(DbError::Message(format!(
            "stored blob object {} already has different identity or bytes",
            remote_object_id(&proposed.identity.object)
        )));
    }
    let proposed_has_owner = match &proposed.state {
        OwnedObjectState::Prepared { ownership } => ownership.pending.contains(owner),
        OwnedObjectState::UploadedVerified { ownership } => ownership.pending.contains(owner),
        OwnedObjectState::RetirementPending { .. } => false,
    };
    if !proposed_has_owner {
        return Err(DbError::Message(format!(
            "stored blob object {} does not name its preparing commit",
            remote_object_id(&proposed.identity.object)
        )));
    }
    let proposed_uploaded = matches!(&proposed.state, OwnedObjectState::UploadedVerified { .. });
    match &mut existing.state {
        OwnedObjectState::Prepared { ownership } => {
            ownership.pending.insert(owner.clone());
            if proposed_uploaded {
                existing.state = OwnedObjectState::UploadedVerified {
                    ownership: crate::sync::remote_object::SharedObjectOwnership {
                        pending: ownership.pending.clone(),
                        activated: std::collections::BTreeSet::new(),
                        nonactivated: ownership.nonactivated.clone(),
                    },
                };
            }
        }
        OwnedObjectState::UploadedVerified { ownership } => {
            ownership.pending.insert(owner.clone());
        }
        OwnedObjectState::RetirementPending { former_candidates } => {
            let ownership = crate::sync::remote_object::PendingCandidateOwnership {
                pending: std::collections::BTreeSet::from([owner.clone()]),
                nonactivated: former_candidates.clone(),
            };
            existing.state = if proposed_uploaded {
                OwnedObjectState::UploadedVerified {
                    ownership: crate::sync::remote_object::SharedObjectOwnership {
                        pending: ownership.pending,
                        activated: std::collections::BTreeSet::new(),
                        nonactivated: ownership.nonactivated,
                    },
                }
            } else {
                OwnedObjectState::Prepared { ownership }
            };
        }
    }
    let merged = RemoteObjectRecord::SharedLiveSet(existing);
    merged.validate().map_err(|error| {
        DbError::Message(format!(
            "merged stored blob object {}: {error}",
            merged.object_id()
        ))
    })?;
    Ok(merged)
}

pub(crate) fn validate_prepared_package_on(
    conn: &Connection,
    write_id: &WriteId,
    expected: &PreparedAudiencePackage,
) -> Result<(), DbError> {
    let audience = expected.package().audience().remote_audience();
    let remote_object_id: String = conn
        .query_row(
            "SELECT remote_object_id
             FROM store_write_packages
             WHERE write_id = ?1 AND audience = ?2",
            rusqlite::params![write_id.as_str(), remote_audience_to_db(&audience)],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    let remote_object_id = remote_object_id.parse().map_err(|error| {
        DbError::Message(format!(
            "stored prepared remote object id is invalid: {error}"
        ))
    })?;
    let actual =
        PreparedAudiencePackage::from_remote(load_remote_object_on(conn, remote_object_id)?)?;
    if actual.package() != expected.package()
        || actual.semantic_bytes() != expected.semantic_bytes()
        || actual.stored_bytes() != expected.stored_bytes()
        || actual.object() != expected.object()
        || actual.remote_object_id() != expected.remote_object_id()
    {
        return Err(DbError::Message(format!(
            "write {write_id} audience {audience:?} already has different prepared package bytes"
        )));
    }
    Ok(())
}

pub(crate) fn validate_prepared_blob_on(
    conn: &Connection,
    write_id: &WriteId,
    expected: &PreparedAudienceBlob,
) -> Result<(), DbError> {
    let locator_hash = expected.blob().locator().locator_hash();
    let remote_object_id = expected.remote_object_id();
    let (stored_locator_hash, spool_path): (String, Option<String>) = conn
        .query_row(
            "SELECT locator_hash, spool_path
             FROM store_write_blobs
             WHERE write_id = ?1 AND audience = ?2 AND remote_object_id = ?3",
            rusqlite::params![
                write_id.as_str(),
                remote_audience_to_db(expected.audience()),
                remote_object_id.to_string(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(DbError::from)?;
    if stored_locator_hash != locator_hash.to_string() {
        return Err(DbError::Message(format!(
            "write {write_id} audience {:?} exact object {remote_object_id} is indexed under locator {stored_locator_hash}, expected {locator_hash}",
            expected.audience()
        )));
    }
    let actual = PreparedAudienceBlob::from_remote(
        expected.audience().clone(),
        &locator_hash.to_string(),
        load_remote_object_on(conn, remote_object_id)?,
        spool_path.map(PathBuf::from),
    )?;
    if actual.blob() != expected.blob()
        || actual.spool_path() != expected.spool_path()
        || actual.remote_object_id() != expected.remote_object_id()
    {
        return Err(DbError::Message(format!(
            "write {write_id} audience {:?} exact object {remote_object_id} already has different prepared blob bytes",
            expected.audience()
        )));
    }
    Ok(())
}

impl Database {
    pub(crate) async fn mark_candidate_cleanup_absent(
        &self,
        object: ExactObjectRef,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let object_id = remote_object_id(&object);
            let mut remote = load_remote_object_on(conn, object_id)?;
            if remote.cleanup_target() != Some(&object) {
                return Err(DbError::Message(format!(
                    "remote object {object_id} is not awaiting exact cleanup"
                )));
            }
            remote.mark_absent_verified().map_err(|error| {
                DbError::Message(format!("mark candidate {object_id} absent: {error}"))
            })?;
            update_remote_object_on(conn, object_id, &remote)
        })
        .await
    }
}
