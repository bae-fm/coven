use super::candidate_records::{
    begin_candidate_nonactivation_targets_on, candidate_cleanup_targets_on,
};
use super::*;
use crate::store::StoreSession;
use crate::*;
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::remote_object::RemoteObjectRecord;
use coven_protocol::store_commit::{ObjectHash, StoreBatchCommitRef};
use rusqlite::OptionalExtension;
use std::collections::BTreeSet;

impl StoreSession<'_> {
    fn outbound_membership_mutation(
        &mut self,
    ) -> Result<Option<DurableMembershipMutation>, DbError> {
        let conn = self.conn;
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
            let intent_hash: ObjectHash = hash
                .parse()
                .map_err(|error| DbError::context("membership intent hash", error))?;
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
    }

    fn select_causal_author_stream(
        &mut self,
        key: &str,
        reusable: &std::collections::BTreeSet<coven_protocol::membership::AuthorStreamId>,
        candidate: coven_protocol::membership::AuthorStreamId,
    ) -> Result<coven_protocol::membership::AuthorStreamId, DbError> {
        let conn = self.conn;
        let existing = crate::get_protocol_state_on(conn, key)?
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
        crate::set_protocol_state_on(conn, key, &selected.to_string())?;
        Ok(selected)
    }

    fn stage_membership_mutation(
        &mut self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        pending_rotation_generation: Option<u64>,
    ) -> Result<ObjectHash, DbError> {
        let conn = self.conn;
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
                super::membership_rotation::stage_pending_rotation_on(
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
        super::membership_rotation::stage_pending_rotation_on(
            &tx,
            pending_rotation_generation,
            intent_hash,
        )?;
        tx.commit().map_err(DbError::from)?;
        Ok(intent_hash)
    }

    fn stage_membership_candidate_mutation(
        &mut self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
        pending_rotation_generation: Option<u64>,
    ) -> Result<ObjectHash, DbError> {
        let conn = self.conn;
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
                if stored != **remote {
                    return Err(DbError::Message(
                        "persisted membership ownership differs from its durable plan".to_string(),
                    ));
                }
            }
            super::membership_rotation::stage_pending_rotation_on(
                &tx,
                pending_rotation_generation,
                intent_hash,
            )?;
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
            persist_exact_remote_object_on(
                &tx,
                self.store_dir,
                remote,
                "membership candidate object",
            )?;
        }
        tx.execute(
            "INSERT INTO outbound_membership_mutation \
             (singleton, intent_hash, plan_bytes, progress_bytes) \
             VALUES (1, ?1, ?2, ?3)",
            rusqlite::params![intent_hash.to_string(), plan_bytes, progress_bytes],
        )
        .map_err(DbError::from)?;
        super::membership_rotation::stage_pending_rotation_on(
            &tx,
            pending_rotation_generation,
            intent_hash,
        )?;
        tx.commit().map_err(DbError::from)?;
        Ok(intent_hash)
    }

    fn update_membership_mutation_progress(
        &mut self,
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        let conn = self.conn;
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
    }

    fn adopt_merge_membership_candidate_head(
        &mut self,
        intent_hash: ObjectHash,
        plan_bytes: Vec<u8>,
        previous: RemoteObjectRecord,
        replacement: coven_protocol::remote_object::ClosedRemoteObject,
        rotation_generation: Option<u64>,
        replacement_hash: ObjectHash,
    ) -> Result<ObjectHash, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let previous_id = previous.object_id();
        let current = load_remote_object_on(&tx, previous_id)?;
        if current != previous {
            return Err(DbError::Message(
                "Merge membership candidate head changed before receipt adoption".to_string(),
            ));
        }
        if !crate::remote_object_records::delete_remote_object_on(&tx, previous_id)? {
            return Err(DbError::Message(
                "prepared Merge membership head disappeared during receipt adoption".to_string(),
            ));
        }
        persist_exact_remote_object_on(
            &tx,
            self.store_dir,
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
                "membership mutation changed before Merge head receipt adoption".to_string(),
            ));
        }
        if let Some(generation) = rotation_generation {
            super::membership_rotation::replace_rotation_candidate_mutation_on(
                &tx,
                intent_hash,
                replacement_hash,
                generation,
            )?;
        }
        tx.commit().map_err(DbError::from)?;
        Ok(replacement_hash)
    }

    fn begin_membership_candidate_nonactivation(
        &mut self,
        intent_hash: ObjectHash,
        candidate: StoreBatchCommitRef,
        candidate_objects: Vec<ExactObjectRef>,
        retained_authorities: Vec<ExactObjectRef>,
        progress_bytes: Vec<u8>,
        nonactivation: coven_protocol::remote_object::CandidateNonactivation,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        let conn = self.conn;
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
        let owned = candidate_objects
            .iter()
            .chain(retained_authorities.iter())
            .cloned()
            .collect::<Vec<_>>();
        let cleanup =
            begin_candidate_nonactivation_targets_on(&tx, &candidate, &owned, &nonactivation)?;
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
        Ok(cleanup)
    }

    fn complete_nonactivating_membership_candidate_mutation(
        &mut self,
        intent_hash: ObjectHash,
        candidate: StoreBatchCommitRef,
        candidate_objects: Vec<ExactObjectRef>,
        retained_authorities: Vec<ExactObjectRef>,
        rotation_generation: Option<u64>,
    ) -> Result<(), DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let mut unique = BTreeSet::new();
        for object in &candidate_objects {
            let object_id = remote_object_id(object);
            if !unique.insert(object_id) {
                return Err(DbError::Message(
                    "nonactivating membership candidate repeats an exact object".to_string(),
                ));
            }
        }
        super::candidate_records::require_candidate_cleanup_complete_on(
            &tx,
            &candidate,
            &candidate_objects,
            "losing membership candidate cleanup is incomplete",
        )?;
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
                        DbError::context(
                            format!("parse nonactivating membership authority {object_id}"),
                            error,
                        )
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
        super::candidate_records::delete_remote_objects_on(
            &tx,
            candidate_objects.iter().map(remote_object_id),
            "losing membership",
        )?;
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
                        DbError::context(
                            format!("parse terminal membership authority {object_id}"),
                            error,
                        )
                    })
                })
                .transpose()?
                .is_some_and(|remote| {
                    matches!(
                        remote,
                        RemoteObjectRecord::RetainedAuthority(
                            coven_protocol::remote_object::RetainedAuthorityRecord {
                                state: coven_protocol::remote_object::RetainedAuthorityObjectState::UncreatedVerified { .. },
                                ..
                            }
                        )
                    )
                });
            if removable {
                crate::remote_object_records::delete_remote_object_on(&tx, object_id)?;
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
            super::membership_rotation::remove_rotation_candidate_on(&tx, intent_hash, generation)?;
        }
        tx.commit().map_err(DbError::from)
    }

    fn membership_candidate_cleanup_targets(
        &mut self,
        intent_hash: ObjectHash,
        candidate: &StoreBatchCommitRef,
        objects: &[ExactObjectRef],
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        let conn = self.conn;
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
        candidate_cleanup_targets_on(conn, candidate, objects)
    }

    fn record_direct_revoke_activation(
        &mut self,
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        generation: u64,
    ) -> Result<(), DbError> {
        let conn = self.conn;
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
        super::membership_rotation::commit_rotation_candidate_on(&tx, intent_hash, generation)?;
        tx.commit().map_err(DbError::from)
    }

    fn complete_membership_mutation(&mut self, intent_hash: ObjectHash) -> Result<(), DbError> {
        let conn = self.conn;
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
    }
}

impl StoreDatabase {
    pub async fn outbound_membership_mutation(
        &self,
    ) -> Result<Option<DurableMembershipMutation>, DbError> {
        self.call_store(|session| session.outbound_membership_mutation())
            .await
    }

    pub async fn select_membership_author_stream(
        &self,
        author_pubkey: &str,
        author_owner_grant: &coven_protocol::membership::MembershipGrantId,
        reusable: std::collections::BTreeSet<coven_protocol::membership::AuthorStreamId>,
    ) -> Result<coven_protocol::membership::AuthorStreamId, DbError> {
        self.select_causal_author_stream(
            format!("membership_author_stream/{author_pubkey}/{author_owner_grant}"),
            reusable,
        )
        .await
    }

    pub async fn select_causal_author_stream(
        &self,
        key: String,
        reusable: std::collections::BTreeSet<coven_protocol::membership::AuthorStreamId>,
    ) -> Result<coven_protocol::membership::AuthorStreamId, DbError> {
        let candidate = coven_protocol::membership::AuthorStreamId::from_digest(
            ObjectHash::digest(self.new_store_write_id().as_str().as_bytes()),
        );
        self.call_store(move |session| {
            session.select_causal_author_stream(&key, &reusable, candidate)
        })
        .await
    }

    pub async fn stage_membership_mutation(
        &self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        pending_rotation_generation: Option<u64>,
    ) -> Result<ObjectHash, DbError> {
        self.call_store(move |session| {
            session.stage_membership_mutation(
                plan_bytes,
                progress_bytes,
                pending_rotation_generation,
            )
        })
        .await
    }

    pub async fn stage_membership_candidate_mutation(
        &self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
        pending_rotation_generation: Option<u64>,
    ) -> Result<ObjectHash, DbError> {
        self.call_store(move |session| {
            session.stage_membership_candidate_mutation(
                plan_bytes,
                progress_bytes,
                remote_objects,
                pending_rotation_generation,
            )
        })
        .await
    }

    pub async fn update_membership_mutation_progress(
        &self,
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.update_membership_mutation_progress(intent_hash, progress_bytes)
        })
        .await
    }

    pub async fn adopt_merge_membership_candidate_head(
        &self,
        intent_hash: ObjectHash,
        plan_bytes: Vec<u8>,
        previous: RemoteObjectRecord,
        replacement: coven_protocol::remote_object::ClosedRemoteObject,
        rotation_generation: Option<u64>,
    ) -> Result<ObjectHash, DbError> {
        let (
            RemoteObjectRecord::RetainedAuthority(previous_head),
            RemoteObjectRecord::RetainedAuthority(replacement_head),
        ) = (&previous, replacement.record())
        else {
            return Err(DbError::Message(
                "Merge membership candidate head adoption received a non-authority object"
                    .to_string(),
            ));
        };
        let (
            coven_protocol::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                reference: previous_ref,
                ..
            },
            coven_protocol::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                reference: replacement_ref,
                ..
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
        let replacement = replacement
            .map_record(|mut record| {
                record.mark_uploaded_verified()?;
                Ok(record)
            })
            .map_err(|error| {
                DbError::context("mark adopted Merge membership head uploaded", error)
            })?;
        let replacement_hash = ObjectHash::digest(&plan_bytes);
        self.call_store(move |session| {
            session.adopt_merge_membership_candidate_head(
                intent_hash,
                plan_bytes,
                previous,
                replacement,
                rotation_generation,
                replacement_hash,
            )
        })
        .await
    }

    pub async fn begin_membership_candidate_nonactivation(
        &self,
        intent_hash: ObjectHash,
        candidate: StoreBatchCommitRef,
        candidate_objects: Vec<ExactObjectRef>,
        retained_authorities: Vec<ExactObjectRef>,
        progress_bytes: Vec<u8>,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
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
        self.call_store(move |session| {
            session.begin_membership_candidate_nonactivation(
                intent_hash,
                candidate,
                candidate_objects,
                retained_authorities,
                progress_bytes,
                nonactivation,
            )
        })
        .await
    }

    pub async fn complete_nonactivating_membership_candidate_mutation(
        &self,
        intent_hash: ObjectHash,
        candidate: StoreBatchCommitRef,
        candidate_objects: Vec<ExactObjectRef>,
        retained_authorities: Vec<ExactObjectRef>,
        rotation_generation: Option<u64>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.complete_nonactivating_membership_candidate_mutation(
                intent_hash,
                candidate,
                candidate_objects,
                retained_authorities,
                rotation_generation,
            )
        })
        .await
    }

    pub async fn membership_candidate_cleanup_targets(
        &self,
        intent_hash: ObjectHash,
        candidate: StoreBatchCommitRef,
        objects: Vec<ExactObjectRef>,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        self.call_store(move |session| {
            session.membership_candidate_cleanup_targets(intent_hash, &candidate, &objects)
        })
        .await
    }

    pub async fn record_direct_revoke_activation(
        &self,
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        generation: u64,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.record_direct_revoke_activation(intent_hash, progress_bytes, generation)
        })
        .await
    }

    pub async fn complete_membership_mutation(
        &self,
        intent_hash: ObjectHash,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.complete_membership_mutation(intent_hash))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stream a key already selected is kept whenever it is still reusable,
    /// so a caller that offers it back does not start a second stream.
    #[tokio::test]
    async fn a_reusable_selected_stream_is_returned_again() {
        let database = StoreDatabase::new(&crate::synthetic_store::open_test_db());
        let key = "circle_roster_author_stream/reselect".to_string();

        let selected = database
            .select_causal_author_stream(key.clone(), std::collections::BTreeSet::new())
            .await
            .expect("mint an author stream for a key holding none");

        assert_eq!(
            database
                .select_causal_author_stream(key, std::collections::BTreeSet::from([selected]))
                .await
                .expect("reselect the durable author stream"),
            selected
        );
    }
}
