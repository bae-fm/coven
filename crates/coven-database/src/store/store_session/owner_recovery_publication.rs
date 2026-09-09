use super::*;
use crate::*;
use coven_protocol::store_commit::{
    StoreBatchCommit, StoreCommitCoord, StoreDeviceRegistration,
    StoreDeviceRegistrationActivationRef, StoreDeviceRegistrationOrigin, VerifiedStoreBatchCommit,
};
use rusqlite::OptionalExtension;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableOwnerRecoveryPublication {
    commit: DurablePreparedProtocolObject,
    history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
}

pub(super) fn complete_owner_recovery_publication_on(
    store: super::StoreTransaction<'_, '_>,
    commit: &VerifiedStoreBatchCommit,
    publication: &crate::AcceptedStoreCommitPublication,
) -> Result<(), DbError> {
    if complete_matching_owner_recovery_publication_on(store, commit, &publication.clone().into())?
    {
        return Ok(());
    }
    Err(DbError::Message(
        "completed Owner recovery has no exact publication journal".into(),
    ))
}

pub(super) fn complete_matching_owner_recovery_publication_on(
    store: super::StoreTransaction<'_, '_>,
    commit: &VerifiedStoreBatchCommit,
    acceptance: &crate::AcceptedStoreCommitEvidence,
) -> Result<bool, DbError> {
    let transaction = store.transaction;
    let stored: Option<(String, String)> = transaction
        .query_row(
            "SELECT registration_hash, publication
             FROM local_owner_recovery_publication WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(DbError::from)?;
    let Some(stored) = stored else {
        return Ok(false);
    };
    if stored.0 != commit.author_registration.registration_hash.to_string() {
        return Ok(false);
    }
    let durable: DurableOwnerRecoveryPublication = serde_json::from_str(&stored.1)
        .map_err(|error| DbError::context("parse completed Owner recovery publication", error))?;
    let active = super::active_store_publication::load_active_store_publication_on(transaction)?
        .ok_or_else(|| {
            DbError::Message("completed Owner recovery has no active Store publication".into())
        })?;
    let attempt = active.attempt()?;
    if durable.commit.semantic_bytes() != commit.value().to_bytes()
        || durable.commit.prepared().reference() != &commit.reference().object
        || acceptance.commit_ref() != commit.reference()
        || active.owner() != &ActiveStorePublicationOwner::OwnerRecovery
        || acceptance.exact_publication().is_some_and(|publication| {
            attempt.entry != *publication.entry()
                || attempt.entry_object != publication.reference().object
        })
    {
        return Err(DbError::Message(
            "completed Owner recovery differs from its exact publication journal".into(),
        ));
    }
    MergeMaterializationTransaction::from_store(store).activate_store_operation_remote_objects(
        commit.reference(),
        &[coven_protocol::remote_object::remote_object_id(
            &commit.reference().object,
        )],
    )?;
    let deleted = transaction
        .execute(
            "DELETE FROM local_owner_recovery_publication
             WHERE singleton = 1 AND registration_hash = ?1 AND publication = ?2",
            (&stored.0, &stored.1),
        )
        .map_err(DbError::from)?;
    if deleted != 1 {
        return Err(DbError::Message(
            "Owner recovery publication changed during completion".into(),
        ));
    }
    super::active_store_publication::clear_active_store_commit_for_owner_on(
        transaction,
        &ActiveStorePublicationOwner::OwnerRecovery,
        commit.reference(),
    )?;
    Ok(true)
}

impl DurableOwnerRecoveryPublication {
    fn from_publication(
        publication: OwnerRecoveryPublication,
    ) -> Result<
        (
            Self,
            coven_protocol::prepared_commit::PreparedStorePublication,
        ),
        DbError,
    > {
        if publication.commit.bytes != publication.commit.value.value().to_bytes() {
            return Err(DbError::Message(
                "Owner recovery publication carries noncanonical semantic bytes".into(),
            ));
        }
        Ok((
            Self {
                commit: DurablePreparedProtocolObject::new(
                    publication.commit.bytes,
                    publication.commit.prepared,
                ),
                history_evidence: publication.history_evidence,
            },
            publication.publication,
        ))
    }
}

impl StoreSession<'_> {
    fn verify_owner_recovery_publication(
        &mut self,
        durable: DurableOwnerRecoveryPublication,
        publication: coven_protocol::prepared_commit::PreparedStorePublication,
    ) -> Result<(OwnerRecoveryPublication, ObjectHash), DbError> {
        let local = self.local_store_device_registration()?.ok_or_else(|| {
            DbError::Message("Owner recovery registration journal is absent".into())
        })?;
        if !matches!(
            local.state,
            LocalDeviceRegistrationState::Created | LocalDeviceRegistrationState::Activated { .. }
        ) {
            return Err(DbError::Message(
                "Owner recovery publication requires created registration objects".into(),
            ));
        }
        let records = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir);
        let root = self
            .verified_store_authority
            .required_root_authority_on(records)?;
        let registration =
            StoreDeviceRegistration::parse_at(&local.registration_bytes, &root, local.device_id)
                .map_err(|error| DbError::context("Owner recovery local registration", error))?;
        let registration_ref =
            coven_protocol::store_commit::StoreDeviceRegistrationRef::from_registration(
                &registration,
                local.prepared.reference().clone(),
            );
        if registration_ref.registration_hash != local.registration_hash {
            return Err(DbError::Message(
                "Owner recovery local registration hash differs from its exact reference".into(),
            ));
        }
        let StoreDeviceRegistrationOrigin::Recovery {
            recovery_id,
            recovery_slot,
            owner_grant,
        } = &registration.origin
        else {
            return Err(DbError::Message(
                "Owner recovery publication has a non-recovery registration".into(),
            ));
        };

        durable
            .commit
            .prepared()
            .reference()
            .verify(durable.commit.prepared().stored_bytes())
            .map_err(|error| DbError::context("Owner recovery exact commit", error))?;
        let decoded: StoreBatchCommit = serde_json::from_slice(durable.commit.semantic_bytes())
            .map_err(|error| DbError::context("Owner recovery commit", error))?;
        let stream_id = coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &registration_ref,
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let coord = StoreCommitCoord {
            stream_id,
            sequence: decoded.seq(),
        };
        let commit = VerifiedStoreBatchCommit::parse_prepared(
            durable.commit.semantic_bytes(),
            root.store_root_hash,
            coord,
            durable.commit.prepared().reference().clone(),
            &registration,
        )
        .map_err(|error| DbError::context("verify Owner recovery commit", error))?;
        let [activation] = commit.device_registrations() else {
            return Err(DbError::Message(
                "Owner recovery commit must carry exactly one registration activation".into(),
            ));
        };
        let StoreDeviceRegistrationActivationRef::Recovery {
            recovery_id: activation_recovery_id,
            node,
        } = &activation.authority
        else {
            return Err(DbError::Message(
                "Owner recovery commit carries another registration authority".into(),
            ));
        };
        if commit.seq() != 1
            || commit.author_registration != registration_ref
            || activation.registration != registration_ref
            || activation_recovery_id != recovery_id
            || node.object.slot() != recovery_slot
            || &node.owner_grant != owner_grant
            || commit.value().to_bytes() != durable.commit.semantic_bytes()
        {
            return Err(DbError::Message(
                "Owner recovery commit differs from its local recovery authority".into(),
            ));
        }
        let proof = durable
            .history_evidence
            .membership_proof
            .as_ref()
            .ok_or_else(|| {
                DbError::Message("Owner recovery has no registration authority head".into())
            })?;
        if !matches!(&proof.entry_value.change, coven_protocol::membership::StoreAuthorityChange::DeviceRegistrationActivation { registration } if registration == activation)
        {
            return Err(DbError::Message(
                "Owner recovery authority entry names another activation".into(),
            ));
        }
        durable
            .history_evidence
            .validate_for(commit.reference(), commit.value())
            .map_err(|error| DbError::context("Owner recovery history evidence", error))?;

        publication
            .verify_commit(&commit)
            .map_err(|error| DbError::context("Owner recovery publication", error))?;
        Ok((
            OwnerRecoveryPublication {
                commit: ExactProtocolObject {
                    value: commit,
                    bytes: durable.commit.semantic_bytes,
                    prepared: durable.commit.prepared,
                },
                publication,
                history_evidence: durable.history_evidence,
            },
            local.registration_hash,
        ))
    }

    fn stage_owner_recovery_publication(
        &mut self,
        publication: OwnerRecoveryPublication,
    ) -> Result<OwnerRecoveryPublication, DbError> {
        let (durable, publication) =
            DurableOwnerRecoveryPublication::from_publication(publication)?;
        let (verified, registration_hash) =
            self.verify_owner_recovery_publication(durable.clone(), publication)?;
        let registration_hash = registration_hash.to_string();
        let encoded = serde_json::to_string(&durable)
            .map_err(|error| DbError::context("serialize Owner recovery publication", error))?;
        let active = ActiveStorePublication::commit(
            ActiveStorePublicationOwner::OwnerRecovery,
            verified.commit.value.write_id.clone(),
            verified.commit.value.author_registration.clone(),
            verified.commit.value.reference().coord.clone(),
            verified.publication.clone(),
        )?;
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let current = super::observed_store_publication::load_store_current_publication_on(&tx)?;
        if current.record() != &verified.publication.previous
            || current.observed_version() != Some(&verified.publication.previous_version)
        {
            return Err(DbError::Message(
                "Owner recovery publication extends another accepted boundary".into(),
            ));
        }
        match super::active_store_publication::claim_active_store_publication_on(&tx, &active)? {
            super::active_store_publication::ActiveStorePublicationClaim::Acquired => {}
            super::active_store_publication::ActiveStorePublicationClaim::AlreadyOwned => {
                return Err(DbError::Message(
                    "Owner recovery owns publication before its journal".into(),
                ));
            }
            super::active_store_publication::ActiveStorePublicationClaim::Occupied(owner) => {
                return Err(DbError::Message(format!(
                    "another local Store operation owns publication: {owner:?}"
                )));
            }
        }
        for remote in verified.remote_objects()? {
            persist_exact_remote_object_on(
                &tx,
                self.store_dir,
                &remote,
                "Owner recovery candidate authority",
            )?;
        }
        crate::store::store_session::StoreRecords::new(&tx, self.store_dir)
            .stage_owner_recovery_publication(&registration_hash, &encoded)?;
        tx.commit().map_err(DbError::from)?;
        Ok(verified)
    }

    fn owner_recovery_publication(&mut self) -> Result<Option<OwnerRecoveryPublication>, DbError> {
        let stored = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .owner_recovery_publication_row()?;
        stored
            .map(|(registration_hash, encoded)| {
                let durable = serde_json::from_str(&encoded)
                    .map_err(|error| DbError::context("parse Owner recovery publication", error))?;
                let active =
                    super::active_store_publication::load_active_store_publication_on(self.conn)?
                        .ok_or_else(|| {
                        DbError::Message(
                            "Owner recovery journal has no active Store publication".into(),
                        )
                    })?;
                if active.owner() != &ActiveStorePublicationOwner::OwnerRecovery {
                    return Err(DbError::Message(
                        "Owner recovery journal differs from the active publication owner".into(),
                    ));
                }
                let (publication, local_registration_hash) =
                    self.verify_owner_recovery_publication(durable, active.attempt()?.clone())?;
                if registration_hash != local_registration_hash.to_string() {
                    return Err(DbError::Message(
                        "Owner recovery publication belongs to another local registration".into(),
                    ));
                }
                Ok(publication)
            })
            .transpose()
    }
}

impl StoreDatabase {
    pub async fn stage_owner_recovery_publication(
        &self,
        publication: OwnerRecoveryPublication,
    ) -> Result<OwnerRecoveryPublication, DbError> {
        self.call_store(move |session| session.stage_owner_recovery_publication(publication))
            .await
    }

    pub async fn owner_recovery_publication(
        &self,
    ) -> Result<Option<OwnerRecoveryPublication>, DbError> {
        self.call_store(|session| session.owner_recovery_publication())
            .await
    }
}
