use super::*;
use crate::sync::store::commit_publication::operation::{
    commit_plan, AdmissionMutationPlan, AuthorizedWriterOperation,
};
use coven_protocol::membership::StoreAuthorityChange;

impl<'storage> AuthorizedWriterOperation<'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn admit_member(
        &mut self,
        public_key_hex: &str,
        member_email: Option<&str>,
        role: coven_protocol::membership::MemberRole,
        encryption: &coven_keys::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<
        crate::sync::store::membership::MemberAdmission,
        crate::sync::store::membership::MembershipOpsError,
    > {
        if role == coven_protocol::membership::MemberRole::Owner {
            return Err(
                crate::sync::store::membership::MembershipOpsError::Mutation(
                    crate::sync::store::membership::MembershipMutationError::Membership(
                        coven_protocol::membership::MembershipError::OwnerPromotionRequired,
                    ),
                ),
            );
        }
        if public_key_hex == self.writer.author_pubkey() {
            return Err(crate::sync::store::membership::MembershipOpsError::SelfAdmission);
        }
        self.resolved_membership()?;
        let root = self.store_root().clone();
        let database = self.database.clone();
        let _mutation = database.membership_mutation_permit().await;
        let pending = database
            .outbound_membership_mutation()
            .await
            .map_err(MembershipMutationError::from)?;
        if pending.is_none() {
            self.refresh_membership_publication()
                .await
                .map_err(MembershipMutationError::from)?;
            self.resolved_membership()?;
            if self.membership.is_member_now(public_key_hex) {
                let wrapped_key = Self::accepted_admission_key(
                    &self.membership,
                    public_key_hex,
                    member_email,
                    &role,
                )?;
                let desired_access = coven_storage::cloud::CloudAccessState::Present {
                    member_pubkey: public_key_hex.to_string(),
                    provider_account_email: member_email.map(str::to_string),
                };
                let outcome = self.storage.set_member_access(desired_access).await?;
                let coven_storage::cloud::CloudAccessOutcome::Present(join_info) = outcome else {
                    return Err(
                        crate::sync::store::membership::MembershipOpsError::ExistingMemberMismatch,
                    );
                };
                let owner_pubkey = self
                    .membership
                    .founder_pubkey()
                    .ok_or(crate::sync::store::membership::MembershipOpsError::ChainHasNoFounder)?
                    .to_string();
                return Ok(crate::sync::store::membership::MemberAdmission {
                    store_id: store_id.to_string(),
                    store_name: store_name.to_string(),
                    join_info,
                    owner_pubkey,
                    wrapped_key,
                    store_root: root,
                    membership_floor: coven_protocol::membership::MembershipFloor(
                        self.membership.head_refs().to_vec(),
                    ),
                });
            }
        }
        let (join_info, wrapped_key) = self
            .continue_admission(pending, public_key_hex, member_email, role, encryption)
            .await?;
        let owner_pubkey = self
            .membership
            .founder_pubkey()
            .ok_or(crate::sync::store::membership::MembershipOpsError::ChainHasNoFounder)?
            .to_string();
        if self.protocol_root().descriptor.store_root_id() != root.store_root_id
            || self.protocol_root().descriptor.founder_pubkey != owner_pubkey
        {
            return Err(crate::sync::store::membership::MembershipOpsError::Chain(
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "Store protocol root differs from the admission authority".to_string(),
                ),
            ));
        }
        Ok(crate::sync::store::membership::MemberAdmission {
            store_id: store_id.to_string(),
            store_name: store_name.to_string(),
            join_info,
            owner_pubkey,
            wrapped_key,
            store_root: root,
            membership_floor: coven_protocol::membership::MembershipFloor(
                self.membership.head_refs().to_vec(),
            ),
        })
    }

    fn accepted_admission_key(
        membership: &coven_protocol::membership::MembershipChain,
        public_key_hex: &str,
        member_email: Option<&str>,
        role: &coven_protocol::membership::MemberRole,
    ) -> Result<
        coven_protocol::wrapped_store_key::WrappedStoreKeyRef,
        crate::sync::store::membership::MembershipOpsError,
    > {
        if !membership
            .current_members()
            .iter()
            .any(|(member, current_role)| member == public_key_hex && current_role == role)
            || membership.current_member_provider_email(public_key_hex) != member_email
        {
            return Err(crate::sync::store::membership::MembershipOpsError::ExistingMemberMismatch);
        }
        let references = membership
            .wrapped_key_authority_for(public_key_hex)
            .map_err(MembershipMutationError::from)?;
        let [reference] = references.as_slice() else {
            return Err(
                crate::sync::store::membership::MembershipOpsError::ExistingMemberKeyAuthority,
            );
        };
        Ok(reference.clone())
    }

    async fn continue_admission(
        &mut self,
        pending: Option<coven_database::DurableMembershipMutation>,
        public_key_hex: &str,
        member_email: Option<&str>,
        role: coven_protocol::membership::MemberRole,
        encryption: &coven_keys::encryption::EncryptionService,
    ) -> Result<
        (
            coven_storage::cloud::CloudHomeJoinInfo,
            coven_protocol::wrapped_store_key::WrappedStoreKeyRef,
        ),
        crate::sync::store::membership::MembershipOpsError,
    > {
        let protocol_store_id = self.store_root().store_root_id.to_string();
        let (mut plan, mut progress, mut intent_hash) = match pending {
            Some(row) => {
                let intent_hash = row.intent_hash;
                let (pending, progress) = decode_membership_mutation(row)?;
                let MembershipMutationPlan::Admission(plan) = pending else {
                    return Err(MembershipMutationError::PendingMutation(
                        "a member removal is pending".into(),
                    )
                    .into());
                };
                if !plan.matches_request(
                    &self.writer_pubkey(),
                    public_key_hex,
                    member_email,
                    &role,
                    &protocol_store_id,
                )? {
                    return Err(MembershipMutationError::PendingMutation(
                        "the pending admission has different immutable inputs".into(),
                    )
                    .into());
                }
                (plan, progress, intent_hash)
            }
            None => {
                let operation_plan = self.prepare_plan().await?;
                let plan = self
                    .prepare_admission_mutation(
                        &operation_plan,
                        public_key_hex,
                        member_email,
                        &role,
                        encryption,
                        self.database.new_store_write_id(),
                    )
                    .await?;
                let remotes = plan
                    .candidate
                    .merge_membership_activation_remote_objects(std::slice::from_ref(
                        &plan.wrapped_key,
                    ))
                    .map_err(MembershipMutationError::from)?;
                let progress = MembershipMutationProgress::Pending;
                let intent_hash = self
                    .database
                    .stage_membership_candidate_mutation(
                        MembershipMutationPlan::Admission(plan.clone()).encode()?,
                        progress.encode()?,
                        remotes,
                        (*plan.candidate).clone(),
                    )
                    .await?;
                (plan, progress, intent_hash)
            }
        };
        loop {
            let publication = plan
                .candidate
                .prepared_membership_publication()
                .map_err(MembershipMutationError::from)?;
            let mut active = self.database.active_store_publication().await?;
            let creation = plan
                .candidate
                .commit
                .membership_authority
                .as_ref()
                .ok_or_else(|| {
                    MembershipMutationError::InvalidDurableMutation(
                        "admission candidate has no initiating authority".into(),
                    )
                })?;
            if self
                .membership
                .write_authority_retirement(creation, &self.writer_pubkey())
                .is_some()
            {
                self.refresh_membership_publication().await?;
                active = self.database.active_store_publication().await?;
                if active
                    .as_ref()
                    .is_some_and(|reserved| reserved.membership_abandonment().is_some())
                {
                    // An existing abandonment uses device authority and can
                    // still settle. Retirement of the original membership grant
                    // cannot prove this abandonment will never activate.
                    active = Some(
                        self.abandon_membership_candidate(intent_hash, &plan.candidate)
                            .await?,
                    );
                }
                if let Some(reserved) = active
                    .as_ref()
                    .filter(|reserved| !reserved.is_awaiting_preparation())
                {
                    let candidate = self
                        .history
                        .authenticate_commit_bytes(
                            &plan.candidate.reference,
                            &plan.candidate.commit.to_bytes(),
                        )
                        .await?;
                    if let Some((membership, publication)) = self
                        .history
                        .candidate_grant_retirement(&candidate)
                        .await
                        .map_err(MembershipMutationError::from)?
                    {
                        active = Some(
                            self.database
                                .retire_membership_candidate_authority(
                                    intent_hash,
                                    reserved.clone(),
                                    (*plan.candidate).clone(),
                                    membership,
                                    publication,
                                )
                                .await?,
                        );
                    }
                }
                if let Some(reserved) = active
                    .as_ref()
                    .filter(|reserved| reserved.is_awaiting_preparation())
                {
                    crate::sync::store::authorization::retire_store_write_candidates(
                        &self.database,
                        self.storage.as_ref(),
                        reserved.clone(),
                    )
                    .await?;
                    let accepted = self
                        .database
                        .store_current_publication()
                        .await?
                        .record()
                        .clone();
                    let satisfaction = if self.membership.is_member_now(public_key_hex) {
                        Some(Self::accepted_admission_key(
                            &self.membership,
                            public_key_hex,
                            member_email,
                            &role,
                        ))
                    } else {
                        None
                    };
                    self.database
                        .complete_retired_membership_authority(
                            intent_hash,
                            reserved.clone(),
                            accepted,
                            self.membership.clone(),
                        )
                        .await?;
                    return match satisfaction {
                        Some(Ok(reference)) => {
                            let join_info = match progress {
                                MembershipMutationProgress::AdmissionGranted { join_info }
                                | MembershipMutationProgress::AdmissionActivated { join_info } => {
                                    join_info
                                }
                                _ => {
                                    return Err(
                                        MembershipMutationError::InitiatingAuthorityRetired.into()
                                    )
                                }
                            };
                            Ok((join_info, reference))
                        }
                        Some(Err(error)) => Err(error),
                        None => Err(MembershipMutationError::InitiatingAuthorityRetired.into()),
                    };
                }
            }
            let continuing = active.as_ref().is_some_and(|active| {
                active.owner() == &coven_database::ActiveStorePublicationOwner::MembershipMutation
                    && (active.is_awaiting_preparation()
                        || active.membership_abandonment().is_some())
            });
            if !continuing {
                match self
                    .publish_admission(plan.clone(), progress.clone(), intent_hash)
                    .await
                {
                    Ok(result) => return Ok(result),
                    Err(error)
                        if super::publication_predecessor_changed(
                            &error,
                            &publication.entry.coord(),
                        ) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            let active = self
                .abandon_membership_candidate(intent_hash, &plan.candidate)
                .await?;
            let row = self
                .database
                .outbound_membership_mutation()
                .await?
                .ok_or_else(|| {
                    MembershipMutationError::InvalidDurableMutation(
                        "retained admission lost its durable journal".into(),
                    )
                })?;
            if row.intent_hash != intent_hash {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "retained admission changed its durable owner".into(),
                )
                .into());
            }
            let (_, retained_progress) = decode_membership_mutation(row)?;
            progress = retained_progress;
            let operation_plan = self.prepare_plan().await?;
            if operation_plan.membership().is_member_now(public_key_hex) {
                let reference = match Self::accepted_admission_key(
                    operation_plan.membership(),
                    public_key_hex,
                    member_email,
                    &role,
                ) {
                    Ok(reference) => reference,
                    Err(
                        crate::sync::store::membership::MembershipOpsError::ExistingMemberMismatch,
                    ) => {
                        let accepted = operation_plan.publication_previous().record().clone();
                        let membership = operation_plan.membership().clone();
                        let access = coven_storage::cloud::CloudAccessState::Present {
                            member_pubkey: public_key_hex.to_string(),
                            provider_account_email: membership
                                .current_member_provider_email(public_key_hex)
                                .map(str::to_string),
                        };
                        drop(operation_plan);
                        if !matches!(
                            self.storage.set_member_access(access).await?,
                            coven_storage::cloud::CloudAccessOutcome::Present(_)
                        ) {
                            return Err(MembershipMutationError::InvalidDurableMutation(
                                "provider did not preserve the accepted member's access".into(),
                            )
                            .into());
                        }
                        self.refresh_membership_publication().await?;
                        self.database
                            .complete_rejected_membership_admission(
                                intent_hash,
                                active,
                                accepted,
                                membership,
                            )
                            .await?;
                        return Err(crate::sync::store::membership::MembershipOpsError::ExistingMemberMismatch);
                    }
                    Err(error) => return Err(error),
                };
                let join_info = self.admission_access(&plan, progress, intent_hash).await?;
                self.database
                    .complete_satisfied_membership_mutation(
                        intent_hash,
                        active,
                        operation_plan.publication_previous().record().clone(),
                        operation_plan.membership().clone(),
                    )
                    .await?;
                return Ok((join_info, reference));
            }
            let replacement = self
                .prepare_admission_mutation(
                    &operation_plan,
                    public_key_hex,
                    member_email,
                    &role,
                    encryption,
                    plan.candidate.commit.write_id.clone(),
                )
                .await?;
            let remotes = replacement
                .candidate
                .merge_membership_activation_remote_objects(std::slice::from_ref(
                    &replacement.wrapped_key,
                ))
                .map_err(MembershipMutationError::from)?;
            intent_hash = self
                .database
                .replace_membership_candidate_mutation(
                    intent_hash,
                    active,
                    (*replacement.candidate).clone(),
                    MembershipMutationPlan::Admission(replacement.clone()).encode()?,
                    MembershipMutationProgress::Pending.encode()?,
                    remotes,
                )
                .await?;
            drop(operation_plan);
            plan = replacement;
            progress = MembershipMutationProgress::Pending;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_admission_mutation(
        &mut self,
        operation_plan: &commit_plan::StoreOperationCommitPlan,
        public_key_hex: &str,
        member_email: Option<&str>,
        role: &coven_protocol::membership::MemberRole,
        encryption: &coven_keys::encryption::EncryptionService,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<AdmissionMutationPlan, MembershipMutationError> {
        let admission_timestamp = self.database.stamp();
        let chain = operation_plan.membership().clone();
        let stream_id = self.select_membership_author_stream(&chain).await?;
        let wrapped_key = self
            .prepare_member_wrapped_key(&chain, encryption, public_key_hex)
            .await?;
        let entry = self.writer.sign_set_member(
            &chain,
            stream_id,
            public_key_hex.to_string(),
            member_email.map(str::to_string),
            role.clone(),
            wrapped_key.reference.clone(),
            admission_timestamp.clone(),
        )?;
        let transition = self.prepare_membership_transition(&chain, entry).await?;
        let mut candidate = self
            .prepare_candidate_for_write(
                operation_plan,
                commit_plan::StoreOperationBatch::MergeMembershipActivation {
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
        self.attach_membership_proof(&mut candidate, &publication)?;
        let plan = AdmissionMutationPlan {
            candidate: Box::new(candidate),
            wrapped_key,
        };
        Ok(plan)
    }

    async fn admission_access(
        &self,
        plan: &AdmissionMutationPlan,
        progress: MembershipMutationProgress,
        intent_hash: coven_protocol::store_commit::ObjectHash,
    ) -> Result<coven_storage::cloud::CloudHomeJoinInfo, MembershipMutationError> {
        match progress {
            MembershipMutationProgress::AdmissionGranted { join_info }
            | MembershipMutationProgress::AdmissionActivated { join_info } => Ok(join_info),
            MembershipMutationProgress::Pending => {
                let publication = plan.candidate.prepared_membership_publication()?;
                let StoreAuthorityChange::SetMember {
                    user_pubkey,
                    provider_account_email,
                    ..
                } = &publication.entry.change
                else {
                    return Err(MembershipMutationError::InvalidDurableMutation(
                        "membership admission plan contains another change".into(),
                    ));
                };
                let outcome = self
                    .storage
                    .set_member_access(coven_storage::cloud::CloudAccessState::Present {
                        member_pubkey: user_pubkey.clone(),
                        provider_account_email: provider_account_email.clone(),
                    })
                    .await?;
                let coven_storage::cloud::CloudAccessOutcome::Present(join_info) = outcome else {
                    return Err(MembershipMutationError::InvalidDurableMutation(
                        "provider returned absent outcome for present access request".into(),
                    ));
                };
                self.membership_mutation_persistence(intent_hash)
                    .record_progress(&MembershipMutationProgress::AdmissionGranted {
                        join_info: join_info.clone(),
                    })
                    .await?;
                Ok(join_info)
            }
            _ => Err(MembershipMutationError::InvalidDurableMutation(
                "admission carries member-removal progress".into(),
            )),
        }
    }

    async fn publish_admission(
        &mut self,
        plan: AdmissionMutationPlan,
        progress: MembershipMutationProgress,
        intent_hash: coven_protocol::store_commit::ObjectHash,
    ) -> Result<
        (
            coven_storage::cloud::CloudHomeJoinInfo,
            coven_protocol::wrapped_store_key::WrappedStoreKeyRef,
        ),
        MembershipMutationError,
    > {
        let publication = plan.candidate.prepared_membership_publication()?;
        let wrapped = plan.wrapped_key.validate()?;
        let StoreAuthorityChange::SetMember {
            user_pubkey,
            wrapped_key,
            ..
        } = &publication.entry.change
        else {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership admission plan contains another change".into(),
            ));
        };
        if wrapped_key != &plan.wrapped_key.reference
            || wrapped.author_pubkey != publication.entry.author_pubkey
            || wrapped
                .verify_and_unwrap(
                    &publication.entry.store_id,
                    user_pubkey,
                    std::iter::once(publication.entry.author_pubkey.as_str()),
                )
                .is_err()
        {
            return Err(
                crate::sync::store::membership::MembershipMutationError::InvalidDurableMutation(
                    "planned admission wrap is not bound to its exact entry, recipient, and author"
                        .to_string(),
                ),
            );
        }
        let persistence = self.membership_mutation_persistence(intent_hash);
        let activated = matches!(
            progress,
            MembershipMutationProgress::AdmissionActivated { .. }
        );
        // Access completion is durable before publication can be attempted.
        // Pending still needs its current authority checked before issuing
        // this provider request; completed steps resume their existing outcome.
        if matches!(progress, MembershipMutationProgress::Pending) {
            self.refresh_membership_publication().await?;
            self.membership
                .validate_publication_predecessor(&publication.entry)?;
        }
        let join_info = self.admission_access(&plan, progress, intent_hash).await?;
        if !activated {
            let remotes =
                plan.candidate
                    .merge_membership_activation_remote_objects(std::slice::from_ref(
                        &plan.wrapped_key,
                    ))?;
            self.publish_membership_authority(
                &publication.transition(),
                std::slice::from_ref(&plan.wrapped_key),
            )
            .await?;
            for object in [
                &plan.wrapped_key.reference.object,
                &publication.entry_ref.object,
            ] {
                persistence
                    .mark_remote_object_uploaded(
                        exact_owned_remote(&remotes, object)?.into_record(),
                    )
                    .await?;
            }
            let accepted = self.publish_membership_activation(
                plan.candidate.clone(),
                coven_protocol::membership_mutation::StoreMembershipJournalCompletion::Mutation {
                    intent_hash,
                    progress_bytes: MembershipMutationProgress::AdmissionActivated { join_info: join_info.clone() }.encode()?,
                    remote_objects: remotes.into_iter().map(|remote| remote.into_record()).collect(),
                },
            ).await?;
            if accepted != plan.candidate.reference {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "membership admission accepted another Store candidate".into(),
                ));
            }
        }
        persistence.complete().await?;
        let wrapped_key = plan.wrapped_key.reference;
        Ok((join_info, wrapped_key))
    }
}
