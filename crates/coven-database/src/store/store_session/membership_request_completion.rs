use super::membership_mutations::{
    consume_retired_membership_candidate_on, membership_rotation_generation,
    require_membership_mutation_on, require_retired_membership_candidate_on,
};
use super::*;
use crate::*;
use coven_protocol::membership::{MembershipChain, StoreAuthorityChange};
use coven_protocol::store_commit::ObjectHash;

enum MembershipRequestCompletion {
    Satisfied,
    RejectedAdmission,
    AuthorityRetired,
}

impl MembershipRequestCompletion {
    fn require(
        &self,
        retired: &RetiredStoreCandidate,
        membership: &MembershipChain,
    ) -> Result<(), DbError> {
        let RetiredStoreCandidateInputs::Membership(original) = &retired.inputs else {
            return Err(DbError::Message(
                "request completion has another candidate domain".into(),
            ));
        };
        let (resolved, failure) = match (self, &original.entry.change) {
            (Self::AuthorityRetired, _) => {
                let commit: coven_protocol::store_commit::StoreBatchCommit =
                    serde_json::from_slice(
                        &retired.nonactivation.candidate().canonical_signed_bytes,
                    )
                    .map_err(|error| DbError::context("retired membership candidate", error))?;
                let creation = commit.membership_authority.as_ref().ok_or_else(|| {
                    DbError::Message(
                        "retired membership candidate has no initiating authority".into(),
                    )
                })?;
                (
                    membership
                        .write_authority_retirement(creation, &original.entry.author_pubkey)
                        .is_some(),
                    "membership request initiating authority remains active",
                )
            }
            (Self::Satisfied, StoreAuthorityChange::RemoveMember { user_pubkey, .. }) => (
                !membership.is_member_now(user_pubkey),
                "retained membership removal is not satisfied",
            ),
            (
                Self::Satisfied | Self::RejectedAdmission,
                StoreAuthorityChange::SetMember {
                    user_pubkey,
                    provider_account_email,
                    role,
                    ..
                },
            ) => {
                let grants = membership.active_grant_ids(user_pubkey);
                let matching = grants.iter().try_fold(true, |matching, grant| {
                    let record = membership.active_grant(grant).ok_or_else(|| {
                        DbError::Message("accepted membership grant is unresolved".into())
                    })?;
                    Ok::<_, DbError>(
                        matching
                            && &record.role == role
                            && &record.provider_account_email == provider_account_email,
                    )
                })?;
                match self {
                    Self::Satisfied => (
                        grants.len() == 1 && matching,
                        "retained membership admission is not satisfied",
                    ),
                    Self::RejectedAdmission => (
                        grants.len() == 1 && !matching,
                        "retained membership admission is not rejected by an accepted grant",
                    ),
                    Self::AuthorityRetired => unreachable!("authority retirement is handled above"),
                }
            }
            _ => {
                return Err(DbError::Message(
                    "membership completion does not match its retained request".into(),
                ))
            }
        };
        if membership.conflict().is_some() || !resolved {
            return Err(DbError::Message(failure.into()));
        }
        Ok(())
    }
}

impl StoreSession<'_> {
    fn retire_membership_candidate_authority(
        &mut self,
        intent_hash: ObjectHash,
        expected: ActiveStorePublication,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        membership: MembershipChain,
        publication: coven_protocol::store_commit::StorePublicationRef,
    ) -> Result<ActiveStorePublication, DbError> {
        candidate.validate_closed_shape()?;
        if expected.owner() != &ActiveStorePublicationOwner::MembershipMutation
            || expected.commit_reservation()
                != Some((
                    &candidate.commit.write_id,
                    &candidate.commit.author_registration,
                    &candidate.reference.coord,
                ))
            || expected.attempt()?.entry.payload
                != coven_protocol::store_commit::StorePublicationPayload::Commit(
                    candidate.reference.clone(),
                )
            || !expected.retired_candidates().is_empty()
        {
            return Err(DbError::Message(
                "membership retirement differs from its reserved candidate".into(),
            ));
        }
        let original = candidate.prepared_membership_publication()?;
        let tx = self.conn.unchecked_transaction()?;
        require_membership_mutation_on(&tx, intent_hash)?;
        crate::remote_object_records::validate_remote_object_on(
            &tx,
            coven_protocol::remote_object::remote_object_id(&candidate.reference.object),
            &candidate.reference.object,
            &candidate.commit.to_bytes(),
        )?;
        let nonactivation = StoreTransaction::new(&tx, self.store_dir)
            .candidate_grant_nonactivation(
                self.verified_store_authority,
                &membership,
                &candidate.reference,
                &candidate.commit,
                &publication,
            )?;
        let mut publications = vec![expected.attempt()?.reference()?];
        if let Some(previous) = expected.superseded_entry() {
            publications.push(previous.clone());
        }
        publications.sort();
        publications.dedup();
        let retired = RetiredStoreCandidate {
            nonactivation,
            inputs: RetiredStoreCandidateInputs::Membership(original),
            publications,
        };
        super::candidate_records::begin_candidate_nonactivation_targets_on(
            &tx,
            &candidate.reference,
            &retired.objects()?,
            &retired.nonactivation,
        )?;
        let replacement = expected.await_preparation(retired)?;
        super::active_store_publication::update_active_store_publication_on(
            &tx,
            &expected,
            &replacement,
        )?;
        tx.commit()?;
        Ok(replacement)
    }

    fn complete_retired_membership_request(
        &mut self,
        completion: MembershipRequestCompletion,
        intent_hash: ObjectHash,
        expected: ActiveStorePublication,
        accepted: coven_protocol::store_commit::StoreCurrentPublicationRecord,
        membership: coven_protocol::membership::MembershipChain,
    ) -> Result<(), DbError> {
        if expected.owner() != &ActiveStorePublicationOwner::MembershipMutation
            || !expected.is_awaiting_preparation()
        {
            return Err(DbError::Message(
                "retained membership request still owns a prepared candidate".into(),
            ));
        }
        let tx = self.conn.unchecked_transaction()?;
        require_membership_mutation_on(&tx, intent_hash)?;
        if super::observed_store_publication::load_store_current_publication_on(&tx)?.record()
            != &accepted
        {
            return Err(DbError::Message(
                "accepted membership changed before completing the retained request".into(),
            ));
        }
        let retired = require_retired_membership_candidate_on(&tx, &expected)?;
        let RetiredStoreCandidateInputs::Membership(original) = &retired.inputs else {
            unreachable!("retired membership candidate is validated")
        };
        let rotation_generation = membership_rotation_generation(&original.entry)?;
        StoreTransaction::new(&tx, self.store_dir).require_accepted_membership(
            self.verified_store_authority,
            &membership,
            accepted.accepted().ok_or_else(|| {
                DbError::Message("retained membership request has no accepted publication".into())
            })?,
        )?;
        completion.require(retired, &membership)?;
        let completed = consume_retired_membership_candidate_on(&tx, &expected)?;
        super::active_store_publication::update_active_store_publication_on(
            &tx, &expected, &completed,
        )?;
        super::active_store_publication::clear_active_store_publication_on(&tx, &completed)?;
        if let Some(generation) = rotation_generation {
            super::membership_rotation::remove_rotation_candidate_on(&tx, intent_hash, generation)?;
        }
        if tx.execute(
            "DELETE FROM outbound_membership_mutation WHERE singleton = 1 AND intent_hash = ?1",
            [intent_hash.to_string()],
        )? != 1
        {
            return Err(DbError::Message(
                "membership mutation changed before completing the retained request".into(),
            ));
        }
        tx.commit()?;
        Ok(())
    }
}

impl StoreDatabase {
    pub async fn retire_membership_candidate_authority(
        &self,
        intent_hash: ObjectHash,
        expected: ActiveStorePublication,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        membership: MembershipChain,
        publication: coven_protocol::store_commit::StorePublicationRef,
    ) -> Result<ActiveStorePublication, DbError> {
        self.call_store(move |session| {
            session.retire_membership_candidate_authority(
                intent_hash,
                expected,
                candidate,
                membership,
                publication,
            )
        })
        .await
    }

    pub async fn complete_retired_membership_authority(
        &self,
        intent_hash: ObjectHash,
        expected: ActiveStorePublication,
        accepted: coven_protocol::store_commit::StoreCurrentPublicationRecord,
        membership: MembershipChain,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.complete_retired_membership_request(
                MembershipRequestCompletion::AuthorityRetired,
                intent_hash,
                expected,
                accepted,
                membership,
            )
        })
        .await
    }

    pub async fn complete_satisfied_membership_mutation(
        &self,
        intent_hash: ObjectHash,
        expected: ActiveStorePublication,
        accepted: coven_protocol::store_commit::StoreCurrentPublicationRecord,
        membership: coven_protocol::membership::MembershipChain,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.complete_retired_membership_request(
                MembershipRequestCompletion::Satisfied,
                intent_hash,
                expected,
                accepted,
                membership,
            )
        })
        .await
    }

    pub async fn complete_rejected_membership_admission(
        &self,
        intent_hash: ObjectHash,
        expected: ActiveStorePublication,
        accepted: coven_protocol::store_commit::StoreCurrentPublicationRecord,
        membership: coven_protocol::membership::MembershipChain,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.complete_retired_membership_request(
                MembershipRequestCompletion::RejectedAdmission,
                intent_hash,
                expected,
                accepted,
                membership,
            )
        })
        .await
    }
}
