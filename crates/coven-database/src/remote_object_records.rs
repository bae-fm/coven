use coven_foundation::store_dir::StoreDir;
use coven_protocol::remote_object::{ClosedRemoteObject, SemanticPayload};

use crate::blob_records::remote_audience_to_db;
use crate::store_reclaim_records::store_reclaim_journal_error;

use super::*;

pub fn candidate_graph_exact_objects(
    commit: &StoreBatchCommit,
) -> Result<Vec<ExactObjectRef>, DbError> {
    coven_protocol::remote_object::CandidateObjectGraph::from_commit(commit)
        .map(|graph| graph.exact_objects().cloned().collect())
        .map_err(|error| DbError::context("closed candidate object graph", error))
}

/// Refuse an indexed remote object that is not the one the index names.
///
/// The comparison is by identity: the exact object, and the hash the record's
/// plaintext is filed under. Payload files are named for the digest of their
/// own contents, so a record whose semantic hash is the digest of these bytes
/// names these bytes.
pub fn validate_remote_object_on(
    conn: &Connection,
    object_id: ObjectHash,
    expected_object: &ExactObjectRef,
    expected_semantic_bytes: &[u8],
) -> Result<(), DbError> {
    let remote = load_remote_object_on(conn, object_id)?;
    let semantic_matches = match remote.semantic_payload() {
        SemanticPayload::Carried(carried) => carried == expected_semantic_bytes,
        SemanticPayload::Spooled(hash) => hash == ObjectHash::digest(expected_semantic_bytes),
        SemanticPayload::Absent => false,
    };
    if remote.object() != expected_object || !semantic_matches {
        return Err(DbError::Message(format!(
            "prepared remote object {object_id} differs from its semantic index"
        )));
    }
    Ok(())
}

pub fn load_remote_object_on(
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
        DbError::context(
            format!("prepared remote object {object_id} has invalid closed state"),
            error,
        )
    })?;
    remote
        .validate()
        .map_err(|error| DbError::context(format!("prepared remote object {object_id}"), error))?;
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

/// Load one record together with the payload files it claims.
///
/// The row and the files are one record; the flows that upload or re-encrypt an
/// object need both halves, and reading them here keeps "the row's claims and
/// the bytes agree" a single check rather than a per-caller convention.
pub fn reopen_remote_object_on(
    records: crate::payload_spool::StoreRecords<'_>,
    object_id: ObjectHash,
) -> Result<coven_protocol::remote_object::ClosedRemoteObject, DbError> {
    let remote = load_remote_object_on(records.conn(), object_id)?;
    let mut payloads = std::collections::BTreeMap::new();
    for hash in remote.payload_claims() {
        let bytes = records
            .payload(hash)
            .map_err(|error| DbError::Message(error.to_string()))?;
        payloads.insert(hash, bytes);
    }
    coven_protocol::remote_object::ClosedRemoteObject::with_payloads(remote, payloads)
        .map_err(|error| DbError::context(format!("remote object {object_id} payloads"), error))
}

pub(crate) fn indexed_retained_replay_owners_on(
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
                DbError::context(
                    format!("retained replay object {object_id} commit ref"),
                    error,
                )
            })?;
        let input_hash = encoded_input_hash.parse().map_err(|error| {
            DbError::context(
                format!("retained replay object {object_id} input hash"),
                error,
            )
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

pub fn index_retained_replay_owner_on(
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
    let commit_ref = serde_json::to_string(commit)
        .map_err(|error| DbError::context("serialize retained replay commit ref", error))?;
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

pub fn load_protocol_inert_object_on(
    conn: &Connection,
    object_id: ObjectHash,
) -> Result<coven_protocol::remote_object::ProtocolInertObject, DbError> {
    let state: String = conn
        .query_row(
            "SELECT state FROM protocol_inert_objects WHERE object_id = ?1",
            [object_id.to_string()],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    let inert: coven_protocol::remote_object::ProtocolInertObject = serde_json::from_str(&state)
        .map_err(|error| {
            DbError::context(
                format!("protocol-inert object {object_id} has invalid closed state"),
                error,
            )
        })?;
    inert
        .validate()
        .map_err(|error| DbError::context(format!("protocol-inert object {object_id}"), error))?;
    if inert.object_id() != object_id {
        return Err(DbError::Message(format!(
            "protocol-inert object key is {object_id}, exact reference hashes to {}",
            inert.object_id()
        )));
    }
    Ok(inert)
}

pub(crate) fn load_reclaimed_store_package_on(
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
        DbError::context(
            format!("reclaimed Store package {object_id} has invalid authorization hash"),
            error,
        )
    })?;
    let reclaimed: ReclaimedStorePackage = serde_json::from_str(&state).map_err(|error| {
        DbError::context(
            format!("reclaimed Store package {object_id} has invalid closed state"),
            error,
        )
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

pub fn record_reclaimed_store_package_on(
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
        let state = serde_json::to_string(reclaimed)
            .map_err(|error| DbError::context("serialize reclaimed Store package", error))?;
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
            coven_protocol::reclaim::ReclaimTarget::StorePackage(target) => {
                remote.validate_reclaimable_store_package(&target.package, &target.activation)
            }
            coven_protocol::reclaim::ReclaimTarget::CirclePackage(target) => {
                remote.validate_reclaimable_circle_package(&target.package, &target.activation)
            }
            coven_protocol::reclaim::ReclaimTarget::CircleBootstrapImage(target) => remote
                .validate_reclaimable_circle_bootstrap_image(
                    &target.coverage.bootstrap.image,
                    &target.coverage.activation_commit,
                ),
            coven_protocol::reclaim::ReclaimTarget::CircleSnapshotImage(target) => {
                let root = required_store_root_authority_on(conn)?;
                let owner = target
                    .snapshot_owner(root.store_root_hash)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                remote.validate_reclaimable_snapshot_image(&target.image, &owner)
            }
            coven_protocol::reclaim::ReclaimTarget::AudienceBlob(target) => {
                remote.validate_reclaimable_stored_blob(&target.blob)
            }
        }
        .map_err(|error| DbError::context(format!("close reclaimed package {object_id}"), error))?;
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
        if !delete_remote_object_on(conn, object_id)? {
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
        .map_err(|error| DbError::context("serialize reclaimed Store package", error))?;
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

/// Install one record's payload files and record its claim on them, in the
/// transaction that writes the row naming them.
///
/// The files land before the row commits and the claim commits with the row, so
/// a row that exists names files that exist, and a transaction that rolls back
/// leaves content-named files no row points at. Writing them here rather than in
/// each producing flow keeps that a fact of one function instead of a convention
/// ten flows have to honour.
fn install_record_payloads_on(
    conn: &Connection,
    store_dir: &StoreDir,
    closed: &ClosedRemoteObject,
) -> Result<(), DbError> {
    for (hash, bytes) in closed.payload_bytes() {
        let written = crate::payload_spool::write_payload_blocking(store_dir, bytes)
            .map_err(|error| DbError::Message(error.to_string()))?;
        if written != *hash {
            return Err(DbError::Message(format!(
                "remote object payload spooled under {written}, named as {hash}"
            )));
        }
    }
    crate::payload_spool::set_payload_owner_claims_on(
        conn,
        &crate::payload_spool::remote_object_owner_key(closed.record().object_id()),
        &closed.payload_bytes().keys().copied().collect(),
    )
}

/// Let go of the payload files one remote object claimed, and remove its row.
///
/// Every deletion of a `remote_objects` row goes through here, so a payload no
/// row names any more is owed its deletion by the same commit. Never used
/// against a projected snapshot copy: those rows describe another device's
/// spool, and deleting them must not touch this one's.
pub(crate) fn delete_remote_object_on(
    conn: &Connection,
    object_id: ObjectHash,
) -> Result<bool, DbError> {
    crate::payload_spool::release_payload_owner_on(
        conn,
        &crate::payload_spool::remote_object_owner_key(object_id),
    )?;
    let removed = conn
        .execute(
            "DELETE FROM remote_objects WHERE object_id = ?1",
            [object_id.to_string()],
        )
        .map_err(DbError::from)?;
    Ok(removed == 1)
}

pub fn persist_exact_remote_object_on(
    conn: &Connection,
    store_dir: &StoreDir,
    closed: &ClosedRemoteObject,
    domain: &str,
) -> Result<(), DbError> {
    let remote = closed.record();
    remote
        .validate()
        .map_err(|error| DbError::context(format!("prepared {domain}"), error))?;
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
            DbError::context(
                format!("prepared {domain} {object_id} has invalid closed state"),
                error,
            )
        })?;
        if existing != *remote {
            return Err(DbError::Message(format!(
                "prepared {domain} {object_id} already has different closed state"
            )));
        }
        return install_record_payloads_on(conn, store_dir, closed);
    }
    install_record_payloads_on(conn, store_dir, closed)?;
    let state = serde_json::to_string(remote)
        .map_err(|error| DbError::context(format!("serialize prepared {domain}"), error))?;
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

pub fn persist_prepared_remote_object_on(
    conn: &Connection,
    store_dir: &StoreDir,
    closed: &ClosedRemoteObject,
    owner: &StoreBatchCommitRef,
    domain: &str,
) -> Result<(), DbError> {
    let remote = closed.record();
    remote
        .validate()
        .map_err(|error| DbError::context(format!("prepared {domain}"), error))?;
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
        return persist_exact_remote_object_on(conn, store_dir, closed, domain);
    }
    let existing = load_remote_object_on(conn, object_id)?;
    let merged = merge_prepared_remote_object(existing, remote, owner)?;
    install_record_payloads_on(conn, store_dir, closed)?;
    update_remote_object_on(conn, object_id, &merged)
}

pub fn update_remote_object_on(
    conn: &Connection,
    object_id: ObjectHash,
    remote: &RemoteObjectRecord,
) -> Result<(), DbError> {
    remote
        .validate()
        .map_err(|error| DbError::context(format!("remote object {object_id}"), error))?;
    if remote.object_id() != object_id {
        return Err(DbError::Message(format!(
            "remote object {object_id} changed its exact identity"
        )));
    }
    let state = serde_json::to_string(remote)
        .map_err(|error| DbError::context("serialize remote object", error))?;
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

pub fn begin_remote_candidate_nonactivation_on(
    conn: &rusqlite::Transaction<'_>,
    object_id: ObjectHash,
    nonactivation: coven_protocol::remote_object::CandidateNonactivation,
) -> Result<Option<ExactObjectRef>, DbError> {
    let mut remote = load_remote_object_on(conn, object_id)?;
    let inert = remote
        .begin_candidate_nonactivation(nonactivation)
        .map_err(|error| {
            DbError::context(
                format!("record candidate nonactivation for {object_id}"),
                error,
            )
        })?;
    finish_remote_candidate_nonactivation_on(conn, object_id, remote, inert)
}

pub fn begin_remote_candidate_nonactivation_with_verified_head_on(
    conn: &rusqlite::Transaction<'_>,
    object_id: ObjectHash,
    nonactivation: coven_protocol::remote_object::CandidateNonactivation,
    head_nonactivation: &coven_protocol::remote_object::VerifiedCandidateHeadNonactivation,
) -> Result<Option<ExactObjectRef>, DbError> {
    let mut remote = load_remote_object_on(conn, object_id)?;
    let inert = remote
        .begin_candidate_nonactivation_with_verified_head_nonactivation(
            nonactivation,
            head_nonactivation,
        )
        .map_err(|error| {
            DbError::context(
                format!("record candidate nonactivation for {object_id}"),
                error,
            )
        })?;
    finish_remote_candidate_nonactivation_on(conn, object_id, remote, inert)
}

pub fn finish_remote_candidate_nonactivation_on(
    conn: &rusqlite::Transaction<'_>,
    object_id: ObjectHash,
    remote: RemoteObjectRecord,
    inert: Option<coven_protocol::remote_object::ProtocolInertObject>,
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
        .map_err(|error| DbError::context(format!("protocol-inert object {object_id}"), error))?;
    let encoded = serde_json::to_string(&inert)
        .map_err(|error| DbError::context("serialize protocol-inert object", error))?;
    if !delete_remote_object_on(conn, object_id)? {
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

pub fn replace_prepared_merge_head_remote_on(
    conn: &Connection,
    store_dir: &StoreDir,
    current: &ExactObjectRef,
    winner: &StoreDeviceHead,
    winner_object: &ExactObjectRef,
    candidate: &StoreBatchCommitRef,
) -> Result<(), DbError> {
    // A Store head is signed plaintext, so the object it is published as names
    // the digest of the head's own canonical bytes.
    let winner_bytes = winner.to_bytes();
    if winner_object.verify(&winner_bytes).is_err()
        || winner_object.slot() != current.slot()
        || winner_object == current
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
                coven_protocol::remote_object::RetainedAuthorityObjectDomain::DeviceHead { .. }
            ) && matches!(
                &record.state,
                coven_protocol::remote_object::RetainedAuthorityObjectState::Prepared { ownership }
                    if ownership.pending == BTreeSet::from([candidate.clone()])
            )
    ) {
        return Err(DbError::Message(
            "prepared Merge head lost its candidate ownership".to_string(),
        ));
    }
    if !delete_remote_object_on(conn, old_object_id)? {
        return Err(DbError::Message(
            "prepared Merge head disappeared during replacement".to_string(),
        ));
    }
    let winner_ref = coven_protocol::store_commit::StoreDeviceHeadRef {
        head_hash: winner.head_hash(),
        object: winner_object.clone(),
    };
    let winner_closed = RemoteObjectRecord::candidate_activated_store_head(
        winner_ref,
        &winner_bytes,
        &winner_bytes,
        candidate.clone(),
    )
    .map_err(|error| DbError::context("alternate Merge head", error))?;
    let winner_closed = winner_closed
        .map_record(|mut record| {
            record.mark_uploaded_verified()?;
            Ok(record)
        })
        .map_err(|error| DbError::context("mark alternate Merge head uploaded", error))?;
    persist_exact_remote_object_on(conn, store_dir, &winner_closed, "alternate Merge head")
}

pub fn mark_remote_object_uploaded_on(
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
            coven_protocol::remote_object::CandidateObjectState::Prepared { ownership }
            | coven_protocol::remote_object::CandidateObjectState::UploadedVerified { ownership } => {
                ownership.pending.iter().next()
            }
            coven_protocol::remote_object::CandidateObjectState::CleanupPending { .. }
            | coven_protocol::remote_object::CandidateObjectState::AbsentVerified { .. } => None,
        };
        if expected_record.identity.domain.shared_destination()
            == Some(current_record.identity.domain.clone())
            && expected_record.identity.semantic_hash == current_record.identity.semantic_hash
            && expected_record.identity.object == current_record.identity.object
            && expected_record.payloads == current_record.payloads
            && expected_owner.is_some_and(|owner| {
                matches!(
                    &current_record.state,
                    coven_protocol::remote_object::OwnedObjectState::UploadedVerified { ownership }
                        if ownership.pending.contains(owner)
                            || ownership.activated.contains(
                                &coven_protocol::remote_object::SharedObjectOwner::StoreCommit(
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
            coven_protocol::remote_object::CandidateObjectState::Prepared { ownership }
            | coven_protocol::remote_object::CandidateObjectState::UploadedVerified { ownership } => {
                ownership.pending.iter().next()
            }
            coven_protocol::remote_object::CandidateObjectState::CleanupPending { .. }
            | coven_protocol::remote_object::CandidateObjectState::AbsentVerified { .. } => None,
        };
        if expected_record.identity.domain.retained_destination()
            == Some(current_record.identity.domain.clone())
            && expected_record.identity.semantic_hash == current_record.identity.semantic_hash
            && expected_record.identity.object == current_record.identity.object
            && expected_record.payloads == current_record.payloads
            && expected_owner.is_some_and(|owner| {
                matches!(
                    &current_record.state,
                    coven_protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified {
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
        DbError::context(format!("mark remote object {object_id} uploaded"), error)
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
        .map_err(|error| DbError::context("serialize expected remote object", error))?;
    let uploaded_json = serde_json::to_string(&uploaded)
        .map_err(|error| DbError::context("serialize uploaded remote object", error))?;
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

pub fn mark_reusable_retained_authority_uploaded_on(
    conn: &Connection,
    expected: RemoteObjectRecord,
) -> Result<RemoteObjectRecord, DbError> {
    let object_id = expected.object_id();
    let RemoteObjectRecord::RetainedAuthority(expected_record) = &expected else {
        return Err(DbError::Message(format!(
            "reusable remote object {object_id} is not retained authority"
        )));
    };
    let coven_protocol::remote_object::RetainedAuthorityObjectState::Prepared {
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
        || current_record.payloads != expected_record.payloads
    {
        return Err(DbError::Message(format!(
            "reusable retained authority {object_id} changed exact identity or bytes"
        )));
    }
    let owns_candidate = match &current_record.state {
        coven_protocol::remote_object::RetainedAuthorityObjectState::Prepared { ownership } => {
            ownership.pending.contains(candidate)
        }
        coven_protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified {
            ownership,
        } => ownership.pending.contains(candidate) || ownership.activated.contains(candidate),
        coven_protocol::remote_object::RetainedAuthorityObjectState::CleanupPending { .. }
        | coven_protocol::remote_object::RetainedAuthorityObjectState::AbsentVerified { .. }
        | coven_protocol::remote_object::RetainedAuthorityObjectState::UncreatedVerified {
            ..
        } => false,
    };
    if !owns_candidate {
        return Err(DbError::Message(format!(
            "reusable retained authority {object_id} does not belong to its upload candidate"
        )));
    }
    let before = current.clone();
    current.mark_uploaded_verified().map_err(|error| {
        DbError::context(
            format!("mark reusable retained authority {object_id} uploaded"),
            error,
        )
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
    use coven_protocol::remote_object::{OwnedObjectState, SharedLiveSetObjectDomain};

    if &existing == proposed {
        return Ok(existing);
    }
    if let (
        RemoteObjectRecord::SharedLiveSet(existing_record),
        RemoteObjectRecord::CandidateExclusive(proposed_record),
    ) = (&existing, proposed)
    {
        let proposed_owner = match &proposed_record.state {
            coven_protocol::remote_object::CandidateObjectState::Prepared { ownership }
            | coven_protocol::remote_object::CandidateObjectState::UploadedVerified { ownership } => {
                ownership.pending.contains(owner)
            }
            coven_protocol::remote_object::CandidateObjectState::CleanupPending { .. }
            | coven_protocol::remote_object::CandidateObjectState::AbsentVerified { .. } => false,
        };
        if proposed_record.identity.domain.shared_destination()
            != Some(existing_record.identity.domain.clone())
            || proposed_record.identity.semantic_hash != existing_record.identity.semantic_hash
            || proposed_record.identity.object != existing_record.identity.object
            || proposed_record.payloads != existing_record.payloads
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
                    ownership: coven_protocol::remote_object::SharedObjectOwnership {
                        pending: BTreeSet::from([owner.clone()]),
                        activated: BTreeSet::new(),
                        nonactivated: former_candidates.clone(),
                    },
                };
            }
        }
        merged.validate().map_err(|error| {
            DbError::context(
                format!("merge shared candidate object {}", proposed.object_id()),
                error,
            )
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
            || proposed_record.payloads != existing_record.payloads
        {
            return Err(DbError::Message(format!(
                "retained candidate object {} already has different identity or bytes",
                proposed.object_id()
            )));
        }
        let proposed_owner = match &proposed_record.state {
            coven_protocol::remote_object::CandidateObjectState::Prepared { ownership }
            | coven_protocol::remote_object::CandidateObjectState::UploadedVerified { ownership } => {
                ownership.pending.contains(owner)
            }
            coven_protocol::remote_object::CandidateObjectState::CleanupPending { .. }
            | coven_protocol::remote_object::CandidateObjectState::AbsentVerified { .. } => false,
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
                DbError::context(
                    format!("merge retained candidate object {}", proposed.object_id()),
                    error,
                )
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
        || existing.payloads != proposed.payloads
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
                    ownership: coven_protocol::remote_object::SharedObjectOwnership {
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
            let ownership = coven_protocol::remote_object::PendingCandidateOwnership {
                pending: std::collections::BTreeSet::from([owner.clone()]),
                nonactivated: former_candidates.clone(),
            };
            existing.state = if proposed_uploaded {
                OwnedObjectState::UploadedVerified {
                    ownership: coven_protocol::remote_object::SharedObjectOwnership {
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
        DbError::context(
            format!("merged stored blob object {}", merged.object_id()),
            error,
        )
    })?;
    Ok(merged)
}

pub fn validate_prepared_package_on(
    conn: &Connection,
    store_dir: &StoreDir,
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
    let remote_object_id = remote_object_id
        .parse()
        .map_err(|error| DbError::context("stored prepared remote object id is invalid", error))?;
    let actual = PreparedAudiencePackage::from_remote(
        store_dir,
        load_remote_object_on(conn, remote_object_id)?,
    )?;
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

pub fn validate_prepared_blob_on(
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
