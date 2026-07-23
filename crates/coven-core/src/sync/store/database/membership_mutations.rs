use super::*;
use crate::database::*;
use crate::sync::remote_object::RemoteObjectRecord;
use crate::sync::storage::ExactObjectRef;
use crate::sync::store_commit::{ObjectHash, StoreBatchCommitRef, StreamActivationId};
use rusqlite::OptionalExtension;
use std::collections::BTreeSet;

impl StoreDatabase {
    pub(crate) async fn outbound_membership_mutation(
        &self,
    ) -> Result<Option<DurableMembershipMutation>, DbError> {
        self.sqlite()
            .call(|conn| {
                conn.query_row(
                    "SELECT intent_hash, plan_bytes, progress_bytes \
                 FROM outbound_membership_mutation WHERE singleton = 1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(DbError::from)?
                .map(|(hash, plan_bytes, progress_bytes)| {
                    let intent_hash: ObjectHash = hash.parse().map_err(|error| {
                        DbError::Message(format!("membership intent hash: {error}"))
                    })?;
                    if ObjectHash::digest(&plan_bytes) != intent_hash {
                        return Err(DbError::Message(
                            "membership intent hash differs from its exact plan bytes".to_string(),
                        ));
                    }
                    Ok(DurableMembershipMutation {
                        intent_hash,
                        plan_bytes,
                        progress_bytes,
                    })
                })
                .transpose()
            })
            .await
    }

    pub(crate) async fn select_membership_author_stream(
        &self,
        author_pubkey: &str,
        author_owner_grant: &crate::sync::membership::MembershipGrantId,
        reusable: std::collections::BTreeSet<crate::sync::membership::AuthorStreamId>,
    ) -> Result<crate::sync::membership::AuthorStreamId, DbError> {
        self.select_causal_author_stream(
            format!("membership_author_stream/{author_pubkey}/{author_owner_grant}"),
            reusable,
        )
        .await
    }

    pub(crate) async fn registered_stream_activation(
        &self,
        activation_id: StreamActivationId,
    ) -> Result<Option<crate::sync::store_commit::RegisteredStreamActivation>, DbError> {
        self.sqlite()
            .call(move |conn| {
                load_registered_stream_activation_on(conn, &activation_id.as_hash().to_string())
            })
            .await
    }

    pub(crate) async fn select_causal_author_stream(
        &self,
        key: String,
        reusable: std::collections::BTreeSet<crate::sync::membership::AuthorStreamId>,
    ) -> Result<crate::sync::membership::AuthorStreamId, DbError> {
        let candidate = crate::sync::membership::AuthorStreamId::from_digest(ObjectHash::digest(
            self.sqlite().id_provider().new_id().as_bytes(),
        ));
        self.sqlite()
            .call(move |conn| {
                let existing = conn
                    .query_row(
                        "SELECT value FROM protocol_state WHERE key = ?1",
                        [&key],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(DbError::from)?
                    .map(|value| {
                        value.parse().map_err(|error| {
                            DbError::Message(format!(
                                "membership author stream state is malformed: {error}"
                            ))
                        })
                    })
                    .transpose()?;
                if let Some(existing) = existing {
                    if reusable.contains(&existing) {
                        return Ok(existing);
                    }
                }
                let selected = reusable.iter().next_back().copied().unwrap_or(candidate);
                conn.execute(
                    "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    rusqlite::params![key, selected.to_string()],
                )
                .map_err(DbError::from)?;
                Ok(selected)
            })
            .await
    }

    pub(crate) async fn stage_membership_mutation(
        &self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        pending_rotation_generation: Option<u64>,
    ) -> Result<ObjectHash, DbError> {
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let intent_hash = ObjectHash::digest(&plan_bytes);
                let existing = tx
                    .query_row(
                        "SELECT intent_hash, plan_bytes FROM outbound_membership_mutation \
                     WHERE singleton = 1",
                        [],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
                    )
                    .optional()
                    .map_err(DbError::from)?;
                if let Some((existing_hash, existing_plan)) = existing {
                    if existing_hash == intent_hash.to_string() && existing_plan == plan_bytes {
                        Self::stage_pending_rotation_on(
                            &tx,
                            pending_rotation_generation,
                            intent_hash,
                        )?;
                        tx.commit().map_err(DbError::from)?;
                        return Ok(intent_hash);
                    }
                    return Err(DbError::Message(
                        "a different membership mutation is already pending".to_string(),
                    ));
                }
                tx.execute(
                    "INSERT INTO outbound_membership_mutation \
                 (singleton, intent_hash, plan_bytes, progress_bytes) \
                 VALUES (1, ?1, ?2, ?3)",
                    rusqlite::params![intent_hash.to_string(), plan_bytes, progress_bytes],
                )
                .map_err(DbError::from)?;
                Self::stage_pending_rotation_on(&tx, pending_rotation_generation, intent_hash)?;
                tx.commit().map_err(DbError::from)?;
                Ok(intent_hash)
            })
            .await
    }

    pub(crate) async fn stage_membership_candidate_mutation(
        &self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<RemoteObjectRecord>,
        pending_rotation_generation: Option<u64>,
    ) -> Result<ObjectHash, DbError> {
        self.sqlite()
            .call(move |conn| {
                let intent_hash = ObjectHash::digest(&plan_bytes);
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let existing = tx
                    .query_row(
                        "SELECT intent_hash, plan_bytes FROM outbound_membership_mutation \
                     WHERE singleton = 1",
                        [],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
                    )
                    .optional()
                    .map_err(DbError::from)?;
                if let Some((existing_hash, existing_plan)) = existing {
                    if existing_hash != intent_hash.to_string() || existing_plan != plan_bytes {
                        return Err(DbError::Message(
                            "a different membership mutation is already pending".to_string(),
                        ));
                    }
                    for remote in &remote_objects {
                        let stored = load_remote_object_on(&tx, remote.object_id())?;
                        if stored != *remote {
                            return Err(DbError::Message(
                                "persisted membership ownership differs from its durable plan"
                                    .to_string(),
                            ));
                        }
                    }
                    Self::stage_pending_rotation_on(&tx, pending_rotation_generation, intent_hash)?;
                    tx.commit().map_err(DbError::from)?;
                    return Ok(intent_hash);
                }
                if remote_objects.is_empty() {
                    return Err(DbError::Message(
                        "membership candidate mutation has no remote ownership graph".to_string(),
                    ));
                }
                let mut object_ids = BTreeSet::new();
                for remote in &remote_objects {
                    if !object_ids.insert(remote.object_id()) {
                        return Err(DbError::Message(
                            "membership candidate mutation repeats a remote object".to_string(),
                        ));
                    }
                    persist_exact_remote_object_on(&tx, remote, "membership candidate object")?;
                }
                tx.execute(
                    "INSERT INTO outbound_membership_mutation \
                 (singleton, intent_hash, plan_bytes, progress_bytes) \
                 VALUES (1, ?1, ?2, ?3)",
                    rusqlite::params![intent_hash.to_string(), plan_bytes, progress_bytes],
                )
                .map_err(DbError::from)?;
                Self::stage_pending_rotation_on(&tx, pending_rotation_generation, intent_hash)?;
                tx.commit().map_err(DbError::from)?;
                Ok(intent_hash)
            })
            .await
    }

    fn stage_pending_rotation_on(
        tx: &rusqlite::Transaction<'_>,
        generation: Option<u64>,
        mutation: ObjectHash,
    ) -> Result<(), DbError> {
        let Some(generation) = generation else {
            return Ok(());
        };
        let existing = Self::load_rotation_gate_on(tx)?;
        let gate = existing
            .as_ref()
            .map(|(_, gate)| gate.clone())
            .unwrap_or_else(crate::sync::cloud_storage::RotationGate::empty)
            .with_candidate(generation, mutation)
            .map_err(DbError::Message)?;
        Self::replace_rotation_gate_on(tx, existing.as_ref(), Some(gate), "candidate staging")
    }

    fn load_rotation_gate_on(
        tx: &rusqlite::Transaction<'_>,
    ) -> Result<Option<(String, crate::sync::cloud_storage::RotationGate)>, DbError> {
        let key = crate::sync::cloud_storage::ROTATION_GATE_STATE_KEY;
        tx.query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [key],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .map(|encoded| {
            let gate =
                serde_json::from_str::<crate::sync::cloud_storage::RotationGate>(&encoded)
                    .map_err(|error| DbError::Message(format!("parse rotation gate: {error}")))?;
            gate.validate().map_err(DbError::Message)?;
            Ok((encoded, gate))
        })
        .transpose()
    }

    fn replace_rotation_gate_on(
        tx: &rusqlite::Transaction<'_>,
        expected: Option<&(String, crate::sync::cloud_storage::RotationGate)>,
        next: Option<crate::sync::cloud_storage::RotationGate>,
        operation: &'static str,
    ) -> Result<(), DbError> {
        let key = crate::sync::cloud_storage::ROTATION_GATE_STATE_KEY;
        let changed = match (expected, next) {
            (Some((expected, _)), Some(next)) => {
                let encoded = serde_json::to_string(&next).map_err(|error| {
                    DbError::Message(format!(
                        "serialize rotation gate during {operation}: {error}"
                    ))
                })?;
                tx.execute(
                    "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                    (&encoded, key, expected),
                )
                .map_err(DbError::from)?
            }
            (Some((expected, _)), None) => tx
                .execute(
                    "DELETE FROM protocol_state WHERE key = ?1 AND value = ?2",
                    (key, expected),
                )
                .map_err(DbError::from)?,
            (None, Some(next)) => {
                let encoded = serde_json::to_string(&next).map_err(|error| {
                    DbError::Message(format!(
                        "serialize rotation gate during {operation}: {error}"
                    ))
                })?;
                tx.execute(
                    "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (key, &encoded),
                )
                .map_err(DbError::from)?
            }
            (None, None) => return Ok(()),
        };
        if changed != 1 {
            return Err(DbError::Message(format!(
                "rotation gate changed during {operation}"
            )));
        }
        Ok(())
    }

    pub(crate) async fn record_peer_rotation(
        &self,
        generation: u64,
    ) -> Result<crate::sync::cloud_storage::RotationGate, DbError> {
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let existing = Self::load_rotation_gate_on(&tx)?;
                let next = existing
                    .as_ref()
                    .map(|(_, gate)| gate.clone())
                    .unwrap_or_else(crate::sync::cloud_storage::RotationGate::empty)
                    .merge_peer_commit(generation)
                    .map_err(DbError::Message)?;
                Self::replace_rotation_gate_on(
                    &tx,
                    existing.as_ref(),
                    Some(next.clone()),
                    "peer rotation recording",
                )?;
                tx.commit().map_err(DbError::from)?;
                Ok(next)
            })
            .await
    }

    pub(crate) async fn complete_peer_rotation_adoption(
        &self,
        adopted_generation: u64,
    ) -> Result<Option<crate::sync::cloud_storage::RotationGate>, DbError> {
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let existing = Self::load_rotation_gate_on(&tx)?.ok_or_else(|| {
                    DbError::Message(
                        "rotation gate is absent during peer rotation adoption".to_string(),
                    )
                })?;
                let next = existing
                    .1
                    .clone()
                    .complete_peer_adoption(adopted_generation)
                    .map_err(DbError::Message)?;
                Self::replace_rotation_gate_on(
                    &tx,
                    Some(&existing),
                    next.clone(),
                    "peer rotation adoption",
                )?;
                tx.commit().map_err(DbError::from)?;
                Ok(next)
            })
            .await
    }

    pub(crate) async fn complete_local_rotation_adoption(
        &self,
        intent_hash: ObjectHash,
        generation: u64,
    ) -> Result<Option<crate::sync::cloud_storage::RotationGate>, DbError> {
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let existing = Self::load_rotation_gate_on(&tx)?.ok_or_else(|| {
                    DbError::Message(
                        "rotation gate is absent during local rotation adoption".to_string(),
                    )
                })?;
                let next = existing
                    .1
                    .clone()
                    .complete_local_adoption(generation, intent_hash)
                    .map_err(DbError::Message)?;
                if tx
                    .execute(
                        "DELETE FROM outbound_membership_mutation \
                     WHERE singleton = 1 AND intent_hash = ?1",
                        [intent_hash.to_string()],
                    )
                    .map_err(DbError::from)?
                    != 1
                {
                    return Err(DbError::Message(
                        "membership mutation changed during local rotation adoption".to_string(),
                    ));
                }
                Self::replace_rotation_gate_on(
                    &tx,
                    Some(&existing),
                    next.clone(),
                    "local rotation adoption",
                )?;
                tx.commit().map_err(DbError::from)?;
                Ok(next)
            })
            .await
    }

    pub(crate) async fn update_membership_mutation_progress(
        &self,
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.sqlite()
            .call(move |conn| {
                let updated = conn
                    .execute(
                        "UPDATE outbound_membership_mutation SET progress_bytes = ?1 \
                     WHERE singleton = 1 AND intent_hash = ?2",
                        rusqlite::params![progress_bytes, intent_hash.to_string()],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "membership mutation ownership row is absent or changed".to_string(),
                    ));
                }
                Ok(())
            })
            .await
    }

    pub(crate) async fn adopt_merge_membership_candidate_head(
        &self,
        intent_hash: ObjectHash,
        plan_bytes: Vec<u8>,
        previous: RemoteObjectRecord,
        mut replacement: RemoteObjectRecord,
        rotation_generation: Option<u64>,
    ) -> Result<ObjectHash, DbError> {
        let (
            RemoteObjectRecord::RetainedAuthority(previous_head),
            RemoteObjectRecord::RetainedAuthority(replacement_head),
        ) = (&previous, &replacement)
        else {
            return Err(DbError::Message(
                "Merge membership candidate head adoption received a non-authority object"
                    .to_string(),
            ));
        };
        let (
            crate::sync::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                reference: previous_ref,
            },
            crate::sync::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                reference: replacement_ref,
            },
        ) = (
            &previous_head.identity.domain,
            &replacement_head.identity.domain,
        )
        else {
            return Err(DbError::Message(
                "Merge membership candidate head adoption received another authority domain"
                    .to_string(),
            ));
        };
        if previous_ref.object.slot() != replacement_ref.object.slot()
            || previous_ref == replacement_ref
        {
            return Err(DbError::Message(
                "adopted Merge membership head does not replace the same exact slot".to_string(),
            ));
        }
        replacement.mark_uploaded_verified().map_err(|error| {
            DbError::Message(format!(
                "mark adopted Merge membership head uploaded: {error}"
            ))
        })?;
        let replacement_hash = ObjectHash::digest(&plan_bytes);
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let previous_id = previous.object_id();
                let current = load_remote_object_on(&tx, previous_id)?;
                if current != previous {
                    return Err(DbError::Message(
                        "Merge membership candidate head changed before receipt adoption"
                            .to_string(),
                    ));
                }
                if tx
                    .execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [previous_id.to_string()],
                    )
                    .map_err(DbError::from)?
                    != 1
                {
                    return Err(DbError::Message(
                        "prepared Merge membership head disappeared during receipt adoption"
                            .to_string(),
                    ));
                }
                persist_exact_remote_object_on(
                    &tx,
                    &replacement,
                    "adopted Merge membership candidate head",
                )?;
                if tx
                    .execute(
                        "UPDATE outbound_membership_mutation
                     SET intent_hash = ?1, plan_bytes = ?2
                     WHERE singleton = 1 AND intent_hash = ?3",
                        rusqlite::params![
                            replacement_hash.to_string(),
                            plan_bytes,
                            intent_hash.to_string()
                        ],
                    )
                    .map_err(DbError::from)?
                    != 1
                {
                    return Err(DbError::Message(
                        "membership mutation changed before Merge head receipt adoption"
                            .to_string(),
                    ));
                }
                if let Some(generation) = rotation_generation {
                    Self::replace_rotation_candidate_mutation_on(
                        &tx,
                        intent_hash,
                        replacement_hash,
                        generation,
                    )?;
                }
                tx.commit().map_err(DbError::from)?;
                Ok(replacement_hash)
            })
            .await
    }

    fn replace_rotation_candidate_mutation_on(
        tx: &rusqlite::Transaction<'_>,
        previous: ObjectHash,
        replacement: ObjectHash,
        generation: u64,
    ) -> Result<(), DbError> {
        let existing = Self::load_rotation_gate_on(tx)?.ok_or_else(|| {
            DbError::Message("rotation gate is absent during candidate replacement".to_string())
        })?;
        let next = existing
            .1
            .clone()
            .replace_candidate_mutation(generation, previous, replacement)
            .map_err(DbError::Message)?;
        Self::replace_rotation_gate_on(tx, Some(&existing), Some(next), "candidate replacement")
    }

    pub(crate) async fn begin_membership_candidate_nonactivation(
        &self,
        intent_hash: ObjectHash,
        candidate: StoreBatchCommitRef,
        candidate_objects: Vec<ExactObjectRef>,
        retained_authorities: Vec<ExactObjectRef>,
        progress_bytes: Vec<u8>,
        nonactivation: crate::sync::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        if nonactivation
            .candidate_reference()
            .map_err(|error| DbError::Message(error.to_string()))?
            != candidate
        {
            return Err(DbError::Message(
                "verified nonactivation names another membership candidate".to_string(),
            ));
        }
        let nonactivation = nonactivation.into_durable();
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let exists: bool = tx
                    .query_row(
                        "SELECT EXISTS(
                         SELECT 1 FROM outbound_membership_mutation
                         WHERE singleton = 1 AND intent_hash = ?1
                     )",
                        [intent_hash.to_string()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                if !exists {
                    return Err(DbError::Message(
                        "membership candidate mutation changed before nonactivation".to_string(),
                    ));
                }
                let mut unique = BTreeSet::new();
                let mut cleanup = Vec::new();
                for object in candidate_objects.iter().chain(retained_authorities.iter()) {
                    let object_id = remote_object_id(object);
                    if !unique.insert(object_id) {
                        return Err(DbError::Message(
                            "membership nonactivation repeats an exact owned object".to_string(),
                        ));
                    }
                    if let Some(target) = begin_remote_candidate_nonactivation_on(
                        &tx,
                        object_id,
                        nonactivation.clone(),
                    )? {
                        cleanup.push(CandidateCleanupObject { object: target });
                    }
                }
                if !candidate_objects.contains(&candidate.object)
                    || !cleanup
                        .iter()
                        .any(|target| target.object == candidate.object)
                {
                    return Err(DbError::Message(
                        "losing membership candidate has no exact commit cleanup target"
                            .to_string(),
                    ));
                }
                let updated = tx
                    .execute(
                        "UPDATE outbound_membership_mutation SET progress_bytes = ?1 \
                     WHERE singleton = 1 AND intent_hash = ?2",
                        rusqlite::params![progress_bytes, intent_hash.to_string()],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "membership candidate mutation changed during nonactivation".to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)?;
                cleanup.sort_by(|left, right| left.object.cmp(&right.object));
                Ok(cleanup)
            })
            .await
    }

    pub(crate) async fn complete_nonactivating_membership_candidate_mutation(
        &self,
        intent_hash: ObjectHash,
        candidate: StoreBatchCommitRef,
        candidate_objects: Vec<ExactObjectRef>,
        retained_authorities: Vec<ExactObjectRef>,
        rotation_generation: Option<u64>,
    ) -> Result<(), DbError> {
        self.sqlite().call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let mut unique = BTreeSet::new();
            for object in &candidate_objects {
                let object_id = remote_object_id(object);
                if !unique.insert(object_id) {
                    return Err(DbError::Message(
                        "nonactivating membership candidate repeats an exact object".to_string(),
                    ));
                }
                let remote = load_remote_object_on(&tx, object_id)?;
                if !remote
                    .candidate_cleanup_complete(&candidate)
                    .map_err(|error| DbError::Message(error.to_string()))?
                {
                    return Err(DbError::Message(format!(
                        "losing membership object {object_id} cleanup is incomplete"
                    )));
                }
            }
            for object in &retained_authorities {
                let object_id = remote_object_id(object);
                if !unique.insert(object_id) {
                    return Err(DbError::Message(
                        "nonactivating membership authority repeats an exact object".to_string(),
                    ));
                }
                let remote = tx
                    .query_row(
                        "SELECT state FROM remote_objects WHERE object_id = ?1",
                        [object_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(DbError::from)?
                    .map(|encoded| {
                        serde_json::from_str::<RemoteObjectRecord>(&encoded).map_err(|error| {
                            DbError::Message(format!(
                                "parse nonactivating membership authority {object_id}: {error}"
                            ))
                        })
                    })
                    .transpose()?;
                match remote {
                    Some(remote) => {
                        if !remote
                            .candidate_cleanup_complete(&candidate)
                            .map_err(|error| DbError::Message(error.to_string()))?
                        {
                            return Err(DbError::Message(format!(
                                "membership authority {object_id} still owns its losing candidate"
                            )));
                        }
                    }
                    None => {
                        let inert = load_protocol_inert_object_on(&tx, object_id)?;
                        if inert.object_id() != object_id {
                            return Err(DbError::Message(
                                "protocol-inert membership authority changed exact identity"
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
            for object in candidate_objects {
                let object_id = remote_object_id(&object);
                if tx
                    .execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [object_id.to_string()],
                    )
                    .map_err(DbError::from)?
                    != 1
                {
                    return Err(DbError::Message(format!(
                        "losing membership object {object_id} disappeared during completion"
                    )));
                }
            }
            for object in retained_authorities {
                let object_id = remote_object_id(&object);
                let removable = tx
                    .query_row(
                        "SELECT state FROM remote_objects WHERE object_id = ?1",
                        [object_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(DbError::from)?
                    .map(|encoded| {
                        serde_json::from_str::<RemoteObjectRecord>(&encoded).map_err(|error| {
                            DbError::Message(format!(
                                "parse terminal membership authority {object_id}: {error}"
                            ))
                        })
                    })
                    .transpose()?
                    .is_some_and(|remote| {
                        matches!(
                            remote,
                            RemoteObjectRecord::RetainedAuthority(
                                crate::sync::remote_object::RetainedAuthorityRecord {
                                    state: crate::sync::remote_object::RetainedAuthorityObjectState::UncreatedVerified { .. },
                                    ..
                                }
                            )
                        )
                    });
                if removable {
                    tx.execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [object_id.to_string()],
                    )
                    .map_err(DbError::from)?;
                }
            }
            if tx
                .execute(
                    "DELETE FROM outbound_membership_mutation \
                     WHERE singleton = 1 AND intent_hash = ?1",
                    [intent_hash.to_string()],
                )
                .map_err(DbError::from)?
                != 1
            {
                return Err(DbError::Message(
                    "membership mutation changed during nonactivation completion".to_string(),
                ));
            }
            if let Some(generation) = rotation_generation {
                Self::remove_rotation_candidate_on(&tx, intent_hash, generation)?;
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    fn remove_rotation_candidate_on(
        tx: &rusqlite::Transaction<'_>,
        intent_hash: ObjectHash,
        generation: u64,
    ) -> Result<(), DbError> {
        let existing = Self::load_rotation_gate_on(tx)?.ok_or_else(|| {
            DbError::Message("rotation gate is absent during candidate loss".to_string())
        })?;
        let next = existing
            .1
            .clone()
            .remove_candidate(generation, intent_hash)
            .map_err(DbError::Message)?;
        Self::replace_rotation_gate_on(tx, Some(&existing), next, "candidate loss")
    }

    pub(crate) async fn membership_candidate_cleanup_targets(
        &self,
        intent_hash: ObjectHash,
        candidate: StoreBatchCommitRef,
        objects: Vec<ExactObjectRef>,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        self.sqlite()
            .call(move |conn| {
                let exists: bool = conn
                    .query_row(
                        "SELECT EXISTS(
                         SELECT 1 FROM outbound_membership_mutation
                         WHERE singleton = 1 AND intent_hash = ?1
                     )",
                        [intent_hash.to_string()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                if !exists {
                    return Err(DbError::Message(
                        "membership mutation changed before candidate cleanup".to_string(),
                    ));
                }
                let mut unique = BTreeSet::new();
                let mut cleanup = Vec::new();
                for object in objects {
                    let object_id = remote_object_id(&object);
                    if !unique.insert(object_id) {
                        return Err(DbError::Message(
                            "membership cleanup repeats an exact candidate object".to_string(),
                        ));
                    }
                    let remote = load_remote_object_on(conn, object_id)?;
                    if let Some(target) = remote.cleanup_target() {
                        cleanup.push(CandidateCleanupObject {
                            object: target.clone(),
                        });
                    } else if !remote
                        .candidate_cleanup_complete(&candidate)
                        .map_err(|error| DbError::Message(error.to_string()))?
                    {
                        return Err(DbError::Message(format!(
                            "membership candidate object {object_id} has no cleanup decision"
                        )));
                    }
                }
                cleanup.sort_by(|left, right| left.object.cmp(&right.object));
                Ok(cleanup)
            })
            .await
    }

    pub(crate) fn record_activated_membership_candidate_mutation_on(
        tx: &rusqlite::Transaction<'_>,
        intent_hash: ObjectHash,
        candidate: &StoreBatchCommitRef,
        objects: &[ExactObjectRef],
        progress_bytes: Vec<u8>,
        activation: MembershipMutationActivation,
    ) -> Result<(), DbError> {
        Self::activate_membership_candidate_graph_on(tx, candidate, objects)?;
        if tx
            .execute(
                "UPDATE outbound_membership_mutation SET progress_bytes = ?1 \
                 WHERE singleton = 1 AND intent_hash = ?2",
                rusqlite::params![progress_bytes, intent_hash.to_string()],
            )
            .map_err(DbError::from)?
            != 1
        {
            return Err(DbError::Message(
                "membership mutation changed during activated recording".to_string(),
            ));
        }
        if let MembershipMutationActivation::Rotation { generation } = activation {
            Self::commit_rotation_candidate_on(tx, intent_hash, generation)?;
        }
        Ok(())
    }

    fn commit_rotation_candidate_on(
        tx: &rusqlite::Transaction<'_>,
        intent_hash: ObjectHash,
        generation: u64,
    ) -> Result<(), DbError> {
        let existing = Self::load_rotation_gate_on(tx)?.ok_or_else(|| {
            DbError::Message("rotation gate is absent during candidate activation".to_string())
        })?;
        let gate = existing
            .1
            .clone()
            .commit_candidate(generation, intent_hash)
            .map_err(DbError::Message)?;
        Self::replace_rotation_gate_on(tx, Some(&existing), Some(gate), "membership activation")
    }

    pub(crate) async fn record_direct_revoke_activation(
        &self,
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        generation: u64,
    ) -> Result<(), DbError> {
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                if tx
                    .execute(
                        "UPDATE outbound_membership_mutation SET progress_bytes = ?1 \
                     WHERE singleton = 1 AND intent_hash = ?2",
                        rusqlite::params![progress_bytes, intent_hash.to_string()],
                    )
                    .map_err(DbError::from)?
                    != 1
                {
                    return Err(DbError::Message(
                        "direct revoke mutation changed during activation".to_string(),
                    ));
                }
                Self::commit_rotation_candidate_on(&tx, intent_hash, generation)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    fn activate_membership_candidate_graph_on(
        tx: &rusqlite::Transaction<'_>,
        candidate: &StoreBatchCommitRef,
        objects: &[ExactObjectRef],
    ) -> Result<(), DbError> {
        let mut unique = BTreeSet::new();
        for object in objects {
            let object_id = remote_object_id(object);
            if !unique.insert(object_id) {
                return Err(DbError::Message(
                    "activated membership graph repeats an exact object".to_string(),
                ));
            }
            let remote = load_remote_object_on(tx, object_id)?;
            let activated = remote.clone().into_activated(candidate).map_err(|error| {
                DbError::Message(format!(
                    "validate activated membership object {object_id}: {error}"
                ))
            })?;
            if activated != remote {
                let expected = serde_json::to_string(&remote).map_err(|error| {
                    DbError::Message(format!(
                        "serialize pending membership object {object_id}: {error}"
                    ))
                })?;
                let replacement = serde_json::to_string(&activated).map_err(|error| {
                    DbError::Message(format!(
                        "serialize activated membership object {object_id}: {error}"
                    ))
                })?;
                if tx
                    .execute(
                        "UPDATE remote_objects SET state = ?1 WHERE object_id = ?2 AND state = ?3",
                        rusqlite::params![replacement, object_id.to_string(), expected],
                    )
                    .map_err(DbError::from)?
                    != 1
                {
                    return Err(DbError::Message(format!(
                        "membership object {object_id} changed during activation"
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn complete_membership_mutation(
        &self,
        intent_hash: ObjectHash,
    ) -> Result<(), DbError> {
        self.sqlite()
            .call(move |conn| {
                let deleted = conn
                    .execute(
                        "DELETE FROM outbound_membership_mutation \
                     WHERE singleton = 1 AND intent_hash = ?1",
                        [intent_hash.to_string()],
                    )
                    .map_err(DbError::from)?;
                if deleted != 1 {
                    return Err(DbError::Message(
                        "membership mutation ownership row is absent or changed".to_string(),
                    ));
                }
                Ok(())
            })
            .await
    }
}
