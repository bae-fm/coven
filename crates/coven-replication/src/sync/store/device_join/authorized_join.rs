use super::history::DeviceJoinHistory;
use super::*;
use coven_protocol::store_commit::DeviceJoinAbandonmentRef;

mod admission;
mod same_principal;

pub use admission::DeviceProviderAccessAdministrator;

/// One device admits a join: it answers the access request, prepares the
/// storage grant, signs the approval, registers the joining device and
/// activates it. Every step below runs against the same journal row, under the
/// provider-administrator grant this device itself holds.
pub(crate) struct AuthorizedJoin<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
    database: StoreDatabase,
    storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
    root: StoreRootRef,
    verified_root: coven_protocol::objects::VerifiedObject<StoreProtocolRoot>,
    membership: coven_protocol::membership::MembershipChain,
    local_writer: std::sync::Arc<crate::sync::store::commit_publication::LocalStoreWriter>,
}

impl<'operation, 'storage> AuthorizedJoin<'operation, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
        database: StoreDatabase,
        storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
        root: StoreRootRef,
        verified_root: coven_protocol::objects::VerifiedObject<StoreProtocolRoot>,
        membership: coven_protocol::membership::MembershipChain,
        local_writer: std::sync::Arc<crate::sync::store::commit_publication::LocalStoreWriter>,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            root,
            verified_root,
            membership,
            local_writer,
        }
    }

    /// Every provider-administrator grant this device itself holds.
    fn provider_admin_grants(
        &self,
    ) -> Result<
        std::collections::BTreeMap<ProviderAdminGrantId, ProviderAdminGrantRecord>,
        DeviceJoinError,
    > {
        let coven_protocol::membership::MembershipStatus::Resolved(resolved) =
            self.membership.status()
        else {
            return Err(DeviceJoinError::MembershipConflict);
        };
        Ok(self
            .local_writer
            .provider_administrator_grants(resolved.provider_admin.combined_state()))
    }

    fn join_history(&mut self) -> DeviceJoinHistory<'_, 'storage> {
        self.writer.join_history()
    }

    fn journal(&self, attempt_id: DeviceJoinAttemptId) -> StoreJoinJournal<OwnerJoinProgress> {
        StoreJoinJournal::new(&self.database, attempt_id)
    }

    async fn prepare_owner_publication(
        &mut self,
        previous: DeviceJoinJournalRecord,
        operation: OwnerJoinPublication,
        plan: crate::sync::store::commit_publication::operation::commit_plan::StoreOperationCommitPlan,
        batch: crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch,
    ) -> Result<PreparedOwnerJoinPublication, DeviceJoinError> {
        let candidate = self.writer.prepare_candidate(&plan, batch).await?;
        self.stage_owner_publication(previous, operation, candidate)
            .await
    }

    async fn prepare_registration_publication(
        &mut self,
        previous: DeviceJoinJournalRecord,
        operation: OwnerJoinPublication,
        plan: crate::sync::store::commit_publication::operation::commit_plan::StoreOperationCommitPlan,
        registration: coven_protocol::store_commit::ActivatedStoreDeviceRegistration,
    ) -> Result<PreparedOwnerJoinPublication, DeviceJoinError> {
        use crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch;
        let transition = self
            .writer
            .prepare_authority_change(
                plan.membership(),
                coven_protocol::membership::StoreAuthorityChange::DeviceRegistrationActivation {
                    registration: registration.activated_reference()?,
                },
            )
            .await
            .map_err(crate::sync::store::StoreError::from)?;
        let batch = match &operation {
            OwnerJoinPublication::SamePrincipalActivation { request } => {
                StoreOperationBatch::SamePrincipalDeviceJoin {
                    attempt_id: request.approval().request.offer.attempt_id,
                    registration: Box::new(registration),
                    transition: transition.transition.clone(),
                }
            }
            OwnerJoinPublication::JoinActivation { .. } => StoreOperationBatch::JoinActivation {
                registration: Box::new(registration),
                transition: transition.transition.clone(),
            },
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        let mut candidate = self.writer.prepare_candidate(&plan, batch).await?;
        let publication = self
            .writer
            .finish_store_membership_transition(transition, candidate.reference.clone())
            .await
            .map_err(crate::sync::store::StoreError::from)?;
        candidate
            .attach_merge_membership_proof_with(&publication, None)
            .map_err(crate::sync::store::StoreError::from)?;
        self.stage_owner_publication(previous, operation, candidate)
            .await
    }

    async fn stage_owner_publication(
        &self,
        previous: DeviceJoinJournalRecord,
        operation: OwnerJoinPublication,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<PreparedOwnerJoinPublication, DeviceJoinError> {
        let prepared = PreparedOwnerJoinPublication {
            operation,
            candidate: Box::new(candidate),
        };
        let durable = self
            .database
            .prepare_owner_device_join_publication(previous, prepared.clone())
            .await?;
        match &*durable.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::StorePublicationPrepared(durable))
                if durable == &prepared =>
            {
                Ok(prepared)
            }
            _ => Err(DeviceJoinError::JournalConflict),
        }
    }

    async fn publish_owner_publication(
        &mut self,
        prepared: PreparedOwnerJoinPublication,
    ) -> Result<StoreBatchCommitRef, DeviceJoinError> {
        let attempt_id = prepared_operation_attempt_id(&prepared.operation);
        if let Some(remote) = prepared
            .authority_remote_object(attempt_id)
            .map_err(crate::sync::store::StoreError::from)?
        {
            let (context, prefix) =
                owner_publication_object_location(self.root.store_root_hash, &prepared.operation);
            let semantic_bytes = remote.semantic_bytes().ok_or_else(|| {
                DeviceJoinError::Provider(
                    "prepared device join authority has no canonical bytes".to_string(),
                )
            })?;
            let stored_bytes = remote.stored_bytes().ok_or_else(|| {
                DeviceJoinError::Provider(
                    "prepared device join authority has no stored bytes".to_string(),
                )
            })?;
            let exact = coven_protocol::objects::PreparedExactObject::new(
                remote.object().clone(),
                stored_bytes.to_vec(),
            )?;
            self.storage
                .create_verified_protocol_object(&context, &exact, &prefix, semantic_bytes)
                .await
                .map_err(DeviceJoinError::Storage)?;
            self.database
                .mark_reusable_retained_authority_uploaded(remote.into_record())
                .await?;
        }
        if matches!(
            &prepared.operation,
            OwnerJoinPublication::SamePrincipalActivation { .. }
                | OwnerJoinPublication::JoinActivation { .. }
        ) {
            let publication = prepared
                .candidate
                .prepared_membership_publication()
                .map_err(crate::sync::store::StoreError::from)?;
            let transition = publication.transition();
            self.writer
                .publish_membership_authority(&transition, &[])
                .await
                .map_err(crate::sync::store::StoreError::from)?;
            let remote_objects = prepared
                .remote_objects(attempt_id)
                .map_err(crate::sync::store::StoreError::from)?
                .into_iter()
                .map(|object| object.into_record())
                .collect();
            let completion =
                coven_protocol::membership_mutation::StoreMembershipJournalCompletion::DeviceJoin {
                    remote_objects,
                };
            self.database
                .mark_remote_object_uploaded(
                    completion
                        .remote_object(&transition.entry_ref.object)
                        .map_err(crate::sync::store::StoreError::from)?,
                )
                .await?;
            return self
                .writer
                .publish_membership_activation(
                    &transition,
                    &publication,
                    prepared.candidate,
                    completion,
                )
                .await
                .map_err(crate::sync::store::StoreError::from)
                .map_err(DeviceJoinError::from);
        }
        let uploaded = self
            .writer
            .upload_prepared(prepared.candidate.clone())
            .await?;
        self.writer
            .activate_uploaded(uploaded)
            .await
            .map_err(DeviceJoinError::from)
    }

    fn verify_device_admission_approval(
        &self,
        approval: &DeviceProviderAdmissionApproval,
    ) -> Result<(), DeviceJoinError> {
        self.local_writer
            .verify_own_device_admission_approval(approval, &self.verified_root)
    }

    pub(crate) async fn begin(
        &self,
        member_pubkey: &str,
    ) -> Result<DeviceJoinOffer, DeviceJoinError> {
        self.require_eligible_member(member_pubkey)?;
        let mut existing = self
            .database
            .device_join_actions()
            .await?
            .into_iter()
            .filter_map(|action| match action {
                DeviceJoinAction::TransferOffer(offer) if offer.member_pubkey == member_pubkey => {
                    Some(offer)
                }
                _ => None,
            });
        if let Some(offer) = existing.next() {
            if existing.next().is_some() {
                return Err(DeviceJoinError::JournalConflict);
            }
            return Ok(offer);
        }
        let owner_pubkey = self.local_writer.author_pubkey();
        let owner_grant = self
            .membership
            .active_owner_grant(&owner_pubkey)
            .ok_or(DeviceJoinError::OwnerAuthorityRequired)?;
        // The device that offers the join is the device that will admit it, so
        // the offer names a provider-administrator grant this device holds. A
        // device holding none cannot grant the joiner storage access and so
        // cannot make the offer at all.
        let provider_admin = self
            .provider_admin_grants()?
            .into_values()
            .next()
            .ok_or(DeviceJoinError::ProviderAdministratorRequired)?;
        let root = self.root.clone();
        let binding = self.storage.provider_binding().await?;
        let attempt_id = self.database.new_device_join_attempt_id();
        // An offer reserves no slots. It used to reserve two, for an attempt
        // file and an outcome file that only restated what the commits carrying
        // them already said, and every join paid both round trips up front.
        let offer = self.local_writer.sign_device_join_offer(
            attempt_id,
            member_pubkey.to_string(),
            root,
            binding.store,
            owner_grant,
            provider_admin,
        )?;
        self.database
            .begin_device_join(DeviceJoinJournalRecord::owner_offered(offer.clone()))
            .await?;
        Ok(offer)
    }

    fn require_eligible_member(&self, member_pubkey: &str) -> Result<(), DeviceJoinError> {
        if self
            .membership
            .current_members()
            .iter()
            .any(|(pubkey, role)| pubkey == member_pubkey && role.can_write())
        {
            Ok(())
        } else {
            Err(DeviceJoinError::MemberNotEligible)
        }
    }

    /// The record for a grant this device holds. A grant held by some other
    /// device is not something this device can admit under: one party answers
    /// the access request, prepares the grant and signs the approval.
    fn resolve_provider_admin(
        &self,
        grant_id: &ProviderAdminGrantId,
    ) -> Result<ProviderAdminGrantRecord, DeviceJoinError> {
        self.provider_admin_grants()?
            .remove(grant_id)
            .ok_or(DeviceJoinError::ProviderAdministratorRequired)
    }

    async fn validate_registration_request(
        &mut self,
        request: &DeviceRegistrationRequest,
    ) -> Result<DeviceJoinOffer, DeviceJoinError> {
        request.verify()?;
        let offer = request.approval().request.offer.as_ref().clone();
        if self.root != offer.store_root {
            return Err(DeviceJoinError::OfferMismatch);
        }
        if !self
            .local_writer
            .is_authored_by_registration(&offer.owner_registration)
        {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        let provider_admin = self.resolve_provider_admin(&offer.provider_admin.grant_id)?;
        if provider_admin != *offer.provider_admin {
            return Err(DeviceJoinError::ProviderAdministratorRequired);
        }
        let administrator = self
            .join_history()
            .load_registration(&provider_admin.administrator)
            .await?
            .value;
        self.verify_device_admission_approval(request.approval())?;
        if let Some(access_grant) = request.approval().access_grant() {
            self.join_history()
                .verify_accepted_provider_access_activation(
                    access_grant,
                    &provider_admin,
                    &administrator,
                )
                .await?;
        }
        if !self.local_writer.is_current_owner(&self.membership) {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        Ok(offer)
    }

    pub(super) async fn abandon(
        &mut self,
        offer: DeviceJoinOffer,
    ) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
        let journal = self.journal(offer.attempt_id);
        let current = journal.current().await?;
        match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(existing)) => {
                return Ok(existing.clone());
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::StorePublicationPrepared(
                prepared,
            )) if matches!(&prepared.operation, OwnerJoinPublication::Abandonment { offer: durable, .. } if durable == &offer) =>
            {
                let activation = self.publish_owner_publication(prepared.clone()).await?;
                let DeviceJoinAttemptDecisionRef::Abandoned(reference) =
                    &prepared.candidate.commit.device_join_attempt_decisions()[0]
                else {
                    return Err(DeviceJoinError::JournalConflict);
                };
                return Ok(DeviceJoinAbandonment {
                    abandonment: reference.clone(),
                    abandonment_activation: activation,
                });
            }
            _ => {}
        }
        if !self
            .local_writer
            .is_authored_by_registration(&offer.owner_registration)
        {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        self.local_writer.verify_device_join_offer(&offer)?;
        if !self.local_writer.is_current_owner(&self.membership) {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        let abandonment_object = self.local_writer.sign_device_join_abandonment(&offer)?;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAbandonment,
        );
        let prefix =
            coven_protocol::store_commit::device_join_abandonment_semantic_prefix(offer.attempt_id);
        // The offer reserves nothing, so the abandonment gets its slot when it
        // is written — which is the rare path, and every join was paying for it.
        let slot = self
            .storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await?;
        let prepared = self.storage.prepare_protocol_object(
            &context,
            slot,
            &prefix,
            abandonment_object.to_bytes(),
        )?;
        let abandonment_ref = DeviceJoinAbandonmentRef {
            attempt_id: offer.attempt_id,
            abandonment_hash: abandonment_object.abandonment_hash(),
            object: prepared.reference().clone(),
        };
        self.local_writer
            .verify_device_join_abandonment(&abandonment_ref, &abandonment_object)?;
        let plan = self.writer.prepare_plan().await?;
        let publication = self
            .prepare_owner_publication(
                current,
                OwnerJoinPublication::Abandonment {
                    offer: offer.clone(),
                    abandonment: abandonment_object,
                },
                plan,
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::Abandonment(abandonment_ref.clone()),
            )
            .await?;
        let activation = self.publish_owner_publication(publication).await?;
        let abandonment = DeviceJoinAbandonment {
            abandonment: abandonment_ref,
            abandonment_activation: activation,
        };
        Ok(abandonment)
    }

    pub(super) async fn accept_registration(
        &mut self,
        request: DeviceRegistrationRequest,
    ) -> Result<ProvisionalDeviceBootstrap, DeviceJoinError> {
        let access_grant = request
            .approval()
            .access_grant()
            .ok_or(DeviceJoinError::ApprovalMismatch)?
            .clone();
        let offer = self.validate_registration_request(&request).await?;
        let journal = self.journal(offer.attempt_id);
        let durable = journal.current().await?;
        match &*durable.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap)) => {
                if *bootstrap.request == request {
                    return Ok(bootstrap.clone());
                }
                return Err(DeviceJoinError::JournalConflict);
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::StorePublicationPrepared(
                prepared,
            )) if matches!(&prepared.operation, OwnerJoinPublication::Attempt { request: durable } if durable == &request) =>
            {
                let activation = self.publish_owner_publication(prepared.clone()).await?;
                return Ok(ProvisionalDeviceBootstrap {
                    request: Box::new(request),
                    publication_authorization: DeviceJoinChallengePublicationAuthorization {
                        attempt_id: offer.attempt_id,
                        attempt_activation: activation,
                    },
                });
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ApprovalPrepared(approval))
                if approval == request.approval() => {}
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(durable))
                if durable == &request => {}
            _ => return Err(DeviceJoinError::JournalConflict),
        }
        let plan = self.writer.prepare_plan().await?;
        #[cfg(any(test, feature = "test-utils"))]
        self.database
            .reach_test_point(coven_database::DatabaseTestPoint::DeviceJoinAttemptPositionHeld)
            .await;
        let cut = plan.predecessor_cut()?;
        if !self
            .join_history()
            .history_cut_covers(&cut, &access_grant.activation)
            .await?
        {
            return Err(DeviceJoinError::ApprovalActivationMissing);
        }
        let requested = if matches!(
            &*durable.progress,
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(_))
        ) {
            durable
        } else {
            journal
                .advance(
                    &durable,
                    OwnerJoinProgress::RegistrationRequested(request.clone()),
                )
                .await?
        };
        // The commit is the attempt. Its predecessor cut is the history the
        // joining device installs from and its membership state is the
        // authority that cut is read under, so a signed file restating both,
        // from the same key that signs the commit, established nothing.
        let publication = self
            .prepare_owner_publication(
                requested,
                OwnerJoinPublication::Attempt {
                    request: request.clone(),
                },
                plan,
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::Attempt(offer.attempt_id),
            )
            .await?;
        let activation = self.publish_owner_publication(publication).await?;
        let bootstrap = ProvisionalDeviceBootstrap {
            request: Box::new(request),
            publication_authorization: DeviceJoinChallengePublicationAuthorization {
                attempt_id: offer.attempt_id,
                attempt_activation: activation,
            },
        };
        Ok(bootstrap)
    }

    pub(super) async fn finalize(
        &mut self,
        completion: DeviceProviderAdmissionCompletion,
    ) -> Result<DeviceJoinActivation, DeviceJoinError> {
        let attempt_id = completion.attempt_id();
        let journal = self.journal(attempt_id);
        let current = journal.current().await?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationPrepared {
            completion: durable_completion,
            activation,
            ..
        }) = &*current.progress
        {
            if durable_completion == &completion {
                return Ok(activation.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::StorePublicationPrepared(
            prepared,
        )) = &*current.progress
        {
            if !matches!(&prepared.operation, OwnerJoinPublication::JoinActivation { completion: durable } if durable == &completion)
            {
                return Err(DeviceJoinError::JournalConflict);
            }
            let activation = self.publish_owner_publication(prepared.clone()).await?;
            return Ok(DeviceJoinActivation {
                attempt_id,
                outcome_activation: activation,
            });
        }
        match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Completed(durable_completion))
                if durable_completion == &completion => {}
            _ => return Err(DeviceJoinError::JournalConflict),
        }
        // Everything this step used to read back from a signed attempt file is
        // in the completion it was handed: the request the joining device
        // signed, and the approval this device signed over it.
        let bootstrap = completion.bootstrap().clone();
        let request = &bootstrap.bootstrap.request;
        let offer = request.approval().request.offer.clone();
        if self.root != offer.store_root {
            return Err(DeviceJoinError::OfferMismatch);
        }
        if !self
            .local_writer
            .is_authored_by_registration(&offer.owner_registration)
        {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        match (&request.approval().admission, &completion) {
            (
                DeviceProviderAdmission::SamePrincipal,
                DeviceProviderAdmissionCompletion::SamePrincipal { .. },
            ) => {}
            (
                DeviceProviderAdmission::CrossPrincipal { challenge, .. },
                DeviceProviderAdmissionCompletion::CrossPrincipal {
                    readiness, receipt, ..
                },
            ) => {
                let registration = self
                    .join_history()
                    .load_registration(&readiness.proof.registration)
                    .await?
                    .value;
                let ack = self
                    .join_history()
                    .load_acknowledgement(&readiness.proof.initial_ack, &registration)
                    .await?;
                let attempt_cut = self
                    .join_history()
                    .load_commit(
                        &bootstrap
                            .bootstrap
                            .publication_authorization
                            .attempt_activation,
                    )
                    .await?
                    .value()
                    .order
                    .predecessor_cut()?;
                readiness.proof.verify(
                    attempt_id,
                    &attempt_cut,
                    &registration,
                    &readiness.proof.initial_ack,
                    &ack,
                )?;
                let provider_admin = self.resolve_provider_admin(&offer.provider_admin.grant_id)?;
                if &provider_admin != offer.provider_admin.as_ref() {
                    return Err(DeviceJoinError::ProviderAdministratorRequired);
                }
                let administrator = self
                    .join_history()
                    .load_registration(&provider_admin.administrator)
                    .await?
                    .value;
                let response_slot = match request.response() {
                    DeviceProviderResponseReservation::CrossPrincipal { response_slot } => {
                        response_slot
                    }
                    DeviceProviderResponseReservation::SamePrincipal => {
                        return Err(DeviceJoinError::AttemptMismatch);
                    }
                };
                let context = coven_protocol::provider::CrossPrincipalResponseContext {
                    challenge: request.approval().request.cross_challenge_context(),
                    expected_registration_hash: request.expected_registration().registration_hash(),
                    response_slot,
                };
                receipt
                    .verify(
                        &context,
                        &offer.provider,
                        &administrator.device_signing_pubkey,
                        &offer.member_pubkey,
                    )
                    .map_err(DeviceJoinError::ProviderProbe)?;
                if &receipt.transcript.challenge != challenge {
                    return Err(DeviceJoinError::AttemptMismatch);
                }
            }
            _ => return Err(DeviceJoinError::AttemptMismatch),
        }
        let registration_prepared = super::prepare_registration_object(
            self.storage.as_ref(),
            request.expected_registration(),
            request.registration_slot().clone(),
        )?;
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            request.expected_registration(),
            registration_prepared.reference().clone(),
        );
        let activated_registration =
            coven_protocol::store_commit::ActivatedStoreDeviceRegistration::verified(
                coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
                    registration_ref,
                    request.expected_registration().clone(),
                )?,
                coven_protocol::store_commit::StoreDeviceRegistrationActivation::Join {
                    attempt_id,
                },
            )?;
        self.writer.refresh_membership_publication().await?;
        let plan = self.writer.prepare_plan().await?;
        let publication = self
            .prepare_registration_publication(
                current,
                OwnerJoinPublication::JoinActivation {
                    completion: completion.clone(),
                },
                plan,
                activated_registration,
            )
            .await?;
        let activation_ref = self.publish_owner_publication(publication).await?;
        let activation = DeviceJoinActivation {
            attempt_id,
            outcome_activation: activation_ref,
        };
        Ok(activation)
    }
}

fn prepared_operation_attempt_id(operation: &OwnerJoinPublication) -> DeviceJoinAttemptId {
    match operation {
        OwnerJoinPublication::ProviderAccessGrant { request, .. } => request.offer.attempt_id,
        OwnerJoinPublication::Attempt { request }
        | OwnerJoinPublication::SamePrincipalActivation { request } => {
            request.approval().request.offer.attempt_id
        }
        OwnerJoinPublication::Abandonment { offer, .. } => offer.attempt_id,
        OwnerJoinPublication::JoinActivation { completion } => completion.attempt_id(),
    }
}

fn owner_publication_object_location(
    store_root_hash: ObjectHash,
    operation: &OwnerJoinPublication,
) -> (coven_protocol::objects::ProtocolObjectContext, String) {
    let (domain, prefix) = match operation {
        OwnerJoinPublication::ProviderAccessGrant { grant, .. } => (
            ProtocolObjectDomain::ProviderAccessGrant,
            coven_protocol::store_commit::provider_access_grant_semantic_prefix(&grant.grant_id),
        ),
        OwnerJoinPublication::Abandonment { offer, .. } => (
            ProtocolObjectDomain::DeviceJoinAbandonment,
            coven_protocol::store_commit::device_join_abandonment_semantic_prefix(offer.attempt_id),
        ),
        OwnerJoinPublication::SamePrincipalActivation { request } => (
            ProtocolObjectDomain::StoreDeviceRegistration,
            coven_protocol::store_commit::registration_semantic_prefix(
                &request.expected_registration().device_id.to_string(),
            ),
        ),
        OwnerJoinPublication::JoinActivation { completion } => (
            ProtocolObjectDomain::StoreDeviceRegistration,
            coven_protocol::store_commit::registration_semantic_prefix(
                &completion
                    .bootstrap()
                    .bootstrap
                    .request
                    .expected_registration()
                    .device_id
                    .to_string(),
            ),
        ),
        OwnerJoinPublication::Attempt { .. } => {
            unreachable!("attempt publications have no authority object")
        }
    };
    (
        coven_protocol::objects::ProtocolObjectContext::signed_plaintext(store_root_hash, domain),
        prefix,
    )
}

impl Store {
    #[doc(hidden)]
    pub(crate) async fn abandon_device_join(
        &self,
        offer: DeviceJoinOffer,
    ) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer.join_operation().abandon(offer).await
    }

    #[doc(hidden)]
    pub(crate) async fn accept_device_registration_request(
        &self,
        request: DeviceRegistrationRequest,
    ) -> Result<ProvisionalDeviceBootstrap, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer.join_operation().accept_registration(request).await
    }

    /// Finish a same-provider join whose activation was already prepared but
    /// whose journal has not reached its completion.
    #[doc(hidden)]
    pub(crate) async fn resume_same_principal_device_join(
        &self,
        request: DeviceRegistrationRequest,
    ) -> Result<SamePrincipalDeviceJoin, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer
            .join_operation()
            .activate_same_principal_join(request)
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn finalize_device_join(
        &self,
        completion: DeviceProviderAdmissionCompletion,
    ) -> Result<DeviceJoinActivation, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer.join_operation().finalize(completion).await
    }
}
