use super::*;
use crate::sync::store::commit_publication::operation::{
    commit_plan, AuthorizedWriterOperation, TransferAdministrationMutationPlan,
};
use coven_protocol::store_commit::StoreDeviceRegistrationRef;

/// Whether `target` names this Store's exact activated registration for its
/// device. A database failure is that failure, not an inactive target.
async fn target_is_active(
    database: &coven_database::StoreDatabase,
    target: &StoreDeviceRegistrationRef,
) -> Result<bool, crate::sync::store::membership::MembershipOpsError> {
    Ok(database
        .activated_store_device_registration_for_device(target.device_id)
        .await
        .map_err(MembershipMutationError::from)?
        .is_some_and(|activated| activated.reference() == target))
}

impl<'storage> AuthorizedWriterOperation<'storage> {
    /// Move provider administration to `target`, an active registered device of
    /// this Store.
    ///
    /// Only the device that resolves as the current administrator can do this,
    /// and the accepted chain is what settles it: the entry rides one Store
    /// commit, and until that commit is accepted nothing about administration
    /// has changed anywhere.
    pub(crate) async fn transfer_provider_administration(
        &mut self,
        target: &StoreDeviceRegistrationRef,
    ) -> Result<(), crate::sync::store::membership::MembershipOpsError> {
        let database = self.database.clone();
        let store_id = self.store_root().store_root_id.to_string();
        let _mutation = database.membership_mutation_permit().await;
        let pending = database
            .outbound_membership_mutation()
            .await
            .map_err(MembershipMutationError::from)?;
        let (mut plan, mut intent_hash) = match pending {
            Some(row) => {
                let intent_hash = row.intent_hash;
                let (pending, _) = decode_membership_mutation(row)?;
                let MembershipMutationPlan::TransferProviderAdministration(plan) = pending else {
                    return Err(MembershipMutationError::PendingMutation(
                        "another membership mutation is pending".to_string(),
                    )
                    .into());
                };
                if !plan.matches_request(&self.writer_pubkey(), target, &store_id)? {
                    return Err(MembershipMutationError::PendingMutation(
                        "the pending provider-administration transfer has a different target"
                            .to_string(),
                    )
                    .into());
                }
                (plan, intent_hash)
            }
            None => {
                self.refresh_membership_publication()
                    .await
                    .map_err(MembershipMutationError::from)?;
                if self.membership.provider_administrator() == target {
                    return Ok(());
                }
                let operation_plan = self.prepare_plan().await?;
                let plan = self
                    .prepare_administration_transfer(
                        &operation_plan,
                        target,
                        database.new_store_write_id(),
                    )
                    .await?;
                let remotes = plan
                    .candidate
                    .merge_membership_activation_remote_objects()
                    .map_err(MembershipMutationError::from)?;
                let intent_hash = database
                    .stage_membership_candidate_mutation(
                        MembershipMutationPlan::TransferProviderAdministration(plan.clone())
                            .encode()?,
                        MembershipMutationProgress::Pending.encode()?,
                        remotes,
                        (*plan.candidate).clone(),
                    )
                    .await?;
                (plan, intent_hash)
            }
        };
        loop {
            let publication = plan
                .candidate
                .prepared_membership_publication()
                .map_err(MembershipMutationError::from)?;
            let active = database.active_store_publication().await?;
            let continuing = active.as_ref().is_some_and(|active| {
                active.owner() == &coven_database::ActiveStorePublicationOwner::MembershipMutation
                    && (active.is_awaiting_preparation()
                        || active.membership_abandonment().is_some())
            });
            if !continuing {
                match self
                    .publish_administration_transfer(plan.clone(), intent_hash)
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(error)
                        if super::publication_predecessor_changed(
                            &error,
                            &publication.entry.coord(),
                        ) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            // Another writer took the position this candidate claimed. Retire
            // the candidate and re-prepare against the authority that actually
            // landed, rather than republishing a commit its predecessor no
            // longer matches.
            let active = self
                .abandon_membership_candidate(intent_hash, &plan.candidate)
                .await?;
            let operation_plan = self.prepare_plan().await?;
            let accepted = operation_plan.publication_previous().record().clone();
            let membership = operation_plan.membership().clone();
            if membership.provider_administrator() == target {
                drop(operation_plan);
                database
                    .complete_satisfied_membership_mutation(
                        intent_hash,
                        active,
                        accepted,
                        membership,
                    )
                    .await?;
                return Ok(());
            }
            let withdrawal = if !operation_plan.is_provider_administrator() {
                Some(crate::sync::store::membership::MembershipOpsError::NotProviderAdministrator)
            } else if !target_is_active(&database, target).await? {
                Some(crate::sync::store::membership::MembershipOpsError::TransferTargetNotActive)
            } else {
                None
            };
            if let Some(error) = withdrawal {
                drop(operation_plan);
                database
                    .complete_withdrawn_membership_transfer(
                        intent_hash,
                        active,
                        accepted,
                        membership,
                    )
                    .await?;
                return Err(error);
            }
            let replacement = self
                .prepare_administration_transfer(
                    &operation_plan,
                    target,
                    plan.candidate.commit.write_id.clone(),
                )
                .await?;
            let remotes = replacement
                .candidate
                .merge_membership_activation_remote_objects()
                .map_err(MembershipMutationError::from)?;
            intent_hash = database
                .replace_membership_candidate_mutation(
                    intent_hash,
                    active,
                    (*replacement.candidate).clone(),
                    MembershipMutationPlan::TransferProviderAdministration(replacement.clone())
                        .encode()?,
                    MembershipMutationProgress::Pending.encode()?,
                    remotes,
                )
                .await?;
            drop(operation_plan);
            plan = replacement;
        }
    }

    async fn prepare_administration_transfer(
        &mut self,
        operation_plan: &commit_plan::StoreOperationCommitPlan,
        target: &StoreDeviceRegistrationRef,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<
        TransferAdministrationMutationPlan,
        crate::sync::store::membership::MembershipOpsError,
    > {
        if !operation_plan.is_provider_administrator() {
            return Err(
                crate::sync::store::membership::MembershipOpsError::NotProviderAdministrator,
            );
        }
        // The chain names authority; the device state is what says the target
        // is a device this Store can still hand administration to.
        if !target_is_active(&self.database, target).await? {
            return Err(
                crate::sync::store::membership::MembershipOpsError::TransferTargetNotActive,
            );
        }
        let timestamp = self.database.stamp();
        let chain = operation_plan.membership().clone();
        let stream_id = self.select_membership_author_stream(&chain).await?;
        let entry = self
            .writer
            .sign_provider_administration_transfer(&chain, stream_id, target.clone(), timestamp)
            .map_err(MembershipMutationError::from)?;
        let transition = self.prepare_membership_transition(&chain, entry).await?;
        let mut candidate = self
            .prepare_candidate_for_write(
                operation_plan,
                commit_plan::StoreOperationBatch::MergeMembershipActivation {
                    entry: transition.entry.clone(),
                    transition: transition.transition.clone(),
                    stream_activations: Vec::new(),
                },
                write_id,
            )
            .await
            .map_err(MembershipMutationError::from)?;
        let publication = self
            .finish_store_membership_transition(transition, candidate.reference.clone())
            .await?;
        candidate
            .attach_merge_membership_proof(&publication)
            .map_err(crate::sync::store::StoreError::from)
            .map_err(MembershipMutationError::from)?;
        Ok(TransferAdministrationMutationPlan {
            candidate: Box::new(candidate),
        })
    }

    async fn publish_administration_transfer(
        &mut self,
        plan: TransferAdministrationMutationPlan,
        intent_hash: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), MembershipMutationError> {
        let publication = plan.candidate.prepared_membership_publication()?;
        let persistence = self.membership_mutation_persistence(intent_hash);
        self.refresh_membership_publication().await?;
        self.membership
            .validate_publication_predecessor(&publication.entry)?;
        let remotes = plan
            .candidate
            .merge_membership_activation_remote_objects()?;
        self.publish_membership_authority(&plan.candidate, &remotes)
            .await?;
        let accepted = self
            .publish_membership_activation(
                plan.candidate.clone(),
                coven_protocol::membership_mutation::StoreMembershipJournalCompletion::Mutation {
                    intent_hash,
                    progress: None,
                    remote_objects: remotes
                        .into_iter()
                        .map(|remote| remote.into_record())
                        .collect(),
                },
            )
            .await?;
        if accepted != plan.candidate.reference {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "provider-administration transfer accepted another Store candidate".into(),
            ));
        }
        persistence.complete().await?;
        Ok(())
    }
}
