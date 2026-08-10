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
    head: DurablePreparedProtocolObject,
    history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
}

pub(super) fn complete_owner_recovery_publication_on(
    transaction: &rusqlite::Transaction<'_>,
    commit: &VerifiedStoreBatchCommit,
    head: &coven_protocol::store_commit::StoreDeviceHead,
    head_object: &coven_protocol::objects::ExactObjectRef,
) -> Result<(), DbError> {
    let stored: (String, String) = transaction
        .query_row(
            "SELECT registration_hash, publication
             FROM local_owner_recovery_publication WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(DbError::from)?;
    let durable: DurableOwnerRecoveryPublication = serde_json::from_str(&stored.1)
        .map_err(|error| DbError::context("parse completed Owner recovery publication", error))?;
    if stored.0 != commit.author_registration.registration_hash.to_string()
        || durable.commit.semantic_bytes() != commit.value().to_bytes()
        || durable.commit.prepared().reference() != &commit.reference().object
        || durable.head.semantic_bytes() != head.to_bytes()
        || durable.head.prepared().reference() != head_object
    {
        return Err(DbError::Message(
            "completed Owner recovery differs from its exact publication journal".into(),
        ));
    }
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
    Ok(())
}

impl DurableOwnerRecoveryPublication {
    fn from_publication(publication: OwnerRecoveryPublication) -> Result<Self, DbError> {
        if publication.commit.bytes != publication.commit.value.value().to_bytes()
            || publication.head.bytes != publication.head.value.to_bytes()
        {
            return Err(DbError::Message(
                "Owner recovery publication carries noncanonical semantic bytes".into(),
            ));
        }
        Ok(Self {
            commit: DurablePreparedProtocolObject::new(
                publication.commit.bytes,
                publication.commit.prepared,
            ),
            head: DurablePreparedProtocolObject::new(
                publication.head.bytes,
                publication.head.prepared,
            ),
            history_evidence: publication.history_evidence,
        })
    }
}

impl StoreSession<'_> {
    fn verify_owner_recovery_publication(
        &mut self,
        durable: DurableOwnerRecoveryPublication,
    ) -> Result<(OwnerRecoveryPublication, ObjectHash), DbError> {
        let local = self.local_store_device_registration()?.ok_or_else(|| {
            DbError::Message("Owner recovery registration journal is absent".into())
        })?;
        if local.state != LocalDeviceRegistrationState::Created {
            return Err(DbError::Message(
                "Owner recovery publication requires created registration objects".into(),
            ));
        }
        let records = self.records;
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
        durable
            .history_evidence
            .validate_for(commit.reference(), commit.value())
            .map_err(|error| DbError::context("Owner recovery history evidence", error))?;

        durable
            .head
            .prepared()
            .reference()
            .verify(durable.head.prepared().stored_bytes())
            .map_err(|error| DbError::context("Owner recovery exact head", error))?;
        let head = coven_protocol::store_commit::StoreDeviceHead::parse_at(
            durable.head.semantic_bytes(),
            root.store_root_hash,
            &registration,
            commit.reference(),
        )
        .map_err(|error| DbError::context("verify Owner recovery head", error))?;
        let coven_protocol::store_commit::DeviceStreamAnchor::StoreAnnouncements { first_slot } =
            &registration.store_commits
        else {
            return Err(DbError::Message(
                "Owner recovery registration has no announcement stream anchor".into(),
            ));
        };
        let activation = registration
            .store_announcement_activation(&registration_ref)
            .map_err(|error| DbError::context("Owner recovery announcement activation", error))?
            .activation_id();
        if durable.head.prepared().reference().slot() != first_slot
            || head.successor.predecessor.is_some()
            || head.successor.activation != activation
            || &head.successor.next_slot == first_slot
            || head.to_bytes() != durable.head.semantic_bytes()
        {
            return Err(DbError::Message(
                "Owner recovery head differs from its first announcement position".into(),
            ));
        }

        Ok((
            OwnerRecoveryPublication {
                commit: ExactProtocolObject {
                    value: commit,
                    bytes: durable.commit.semantic_bytes,
                    prepared: durable.commit.prepared,
                },
                head: ExactProtocolObject {
                    value: head,
                    bytes: durable.head.semantic_bytes,
                    prepared: durable.head.prepared,
                },
                history_evidence: durable.history_evidence,
            },
            local.registration_hash,
        ))
    }

    fn stage_owner_recovery_publication(
        &mut self,
        publication: OwnerRecoveryPublication,
    ) -> Result<OwnerRecoveryPublication, DbError> {
        let durable = DurableOwnerRecoveryPublication::from_publication(publication)?;
        let (verified, registration_hash) =
            self.verify_owner_recovery_publication(durable.clone())?;
        let registration_hash = registration_hash.to_string();
        let encoded = serde_json::to_string(&durable)
            .map_err(|error| DbError::context("serialize Owner recovery publication", error))?;
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO local_owner_recovery_publication
                 (singleton, registration_hash, publication)
             VALUES (1, ?1, ?2)
             ON CONFLICT(singleton) DO NOTHING",
            (&registration_hash, &encoded),
        )
        .map_err(DbError::from)?;
        let stored: (String, String) = tx
            .query_row(
                "SELECT registration_hash, publication
                 FROM local_owner_recovery_publication WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        if stored != (registration_hash, encoded) {
            return Err(DbError::Message(
                "Owner recovery publication journal owns different exact objects".into(),
            ));
        }
        tx.commit().map_err(DbError::from)?;
        Ok(verified)
    }

    fn owner_recovery_publication(&mut self) -> Result<Option<OwnerRecoveryPublication>, DbError> {
        let stored: Option<(String, String)> = self
            .records
            .conn
            .query_row(
                "SELECT registration_hash, publication
                 FROM local_owner_recovery_publication WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        stored
            .map(|(registration_hash, encoded)| {
                let durable = serde_json::from_str(&encoded)
                    .map_err(|error| DbError::context("parse Owner recovery publication", error))?;
                let (publication, local_registration_hash) =
                    self.verify_owner_recovery_publication(durable)?;
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
