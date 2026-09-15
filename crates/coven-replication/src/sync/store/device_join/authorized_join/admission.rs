use super::*;

impl<'operation, 'storage> AuthorizedJoin<'operation, 'storage> {
    fn sign_device_admission_approval(
        &self,
        request: DeviceProviderAccessRequest,
        admission: DeviceProviderAdmission,
    ) -> Result<DeviceProviderAdmissionApproval, DeviceJoinError> {
        self.local_writer
            .sign_device_admission_approval(request, admission, &self.verified_root)
    }

    async fn publish_cross_principal_challenge(
        &mut self,
        authorization: &DeviceJoinChallengePublicationAuthorization,
        challenge: &CrossPrincipalProbeChallenge,
        context: &coven_protocol::provider::CrossPrincipalChallengeContext,
        store: &StoreProviderBinding,
        attempt_owner: &StoreDeviceRegistration,
    ) -> Result<CrossPrincipalProbeChallenge, DeviceJoinError> {
        self.local_writer
            .verify_cross_principal_challenge(challenge, context, store)
            .map_err(DeviceJoinError::ProviderProbe)?;
        if authorization.attempt_id != context.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        // The commit that opened the attempt is what the challenge is
        // authorized against; there is no separate attempt file to agree with.
        let activation = self
            .join_history()
            .load_commit(&authorization.attempt_activation)
            .await?;
        if activation.author() != attempt_owner
            || !activation
                .device_join_attempt_decisions()
                .iter()
                .any(|decision| {
                    matches!(
                        decision,
                        DeviceJoinAttemptDecisionRef::Attempt(opened)
                            if *opened == authorization.attempt_id
                    )
                })
        {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        self.storage
            .settle_cross_principal_challenge(
                &self.database,
                authorization,
                challenge,
                context,
                store,
            )
            .await
            .map_err(DeviceJoinError::ProviderProbe)
    }

    pub(crate) async fn authorize_access(
        &mut self,
        request: DeviceProviderAccessRequest,
        access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
    ) -> Result<DeviceProviderAdmissionApproval, DeviceJoinError> {
        let provider_admin = self.resolve_provider_admin(&request.offer.provider_admin.grant_id)?;
        if provider_admin != *request.offer.provider_admin {
            return Err(DeviceJoinError::OfferMismatch);
        }
        let owner = self
            .join_history()
            .load_registration(&request.offer.owner_registration)
            .await?
            .value;
        request.verify(&owner)?;
        if !self
            .local_writer
            .is_authored_by_registration(&provider_admin.administrator)
        {
            return Err(DeviceJoinError::ProviderAdministratorRequired);
        }
        let database = self.database.clone();
        let journal = self.journal(request.offer.attempt_id);
        let current = journal.current().await?;
        let durable = match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(_)) => {
                journal
                    .advance(
                        &current,
                        OwnerJoinProgress::AccessRequested(request.clone()),
                    )
                    .await?
            }
            _ => current,
        };
        if provider_admin.provider == request.peer_provider {
            return match &*durable.progress {
                DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ApprovalPrepared(approval)) => {
                    Ok(approval.clone())
                }
                DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AccessRequested(
                    durable_request,
                )) if durable_request == &request => {
                    let approval = self.sign_device_admission_approval(
                        request,
                        DeviceProviderAdmission::SamePrincipal,
                    )?;
                    journal
                        .advance(
                            &durable,
                            OwnerJoinProgress::ApprovalPrepared(approval.clone()),
                        )
                        .await?;
                    Ok(approval)
                }
                _ => Err(DeviceJoinError::JournalConflict),
            };
        }
        // The physical provider authority is created once and recorded before
        // anything else can fail. Its locator is the administrator's own result;
        // the approval this device signs below is what attests it, so there is
        // no separate accepted grant object to publish or read back.
        let (locator, granted) = match &*durable.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ApprovalPrepared(approval)) => {
                return Ok(approval.clone())
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AccessGranted {
                request: durable_request,
                locator,
            }) if durable_request == &request => (locator.clone(), durable.clone()),
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AccessRequested(durable_request))
                if durable_request == &request =>
            {
                let administrator =
                    access_administrator.ok_or(DeviceJoinError::ProviderAdministratorRequired)?;
                let locator = administrator
                    .grant_member_access(
                        &request.offer.member_pubkey,
                        self.membership
                            .current_member_provider_email(&request.offer.member_pubkey),
                        &request.peer_provider,
                    )
                    .await?;
                locator
                    .validate_for(&request.offer.provider, &request.peer_provider)
                    .map_err(DeviceJoinError::Storage)?;
                let granted = journal
                    .advance(
                        &durable,
                        OwnerJoinProgress::AccessGranted {
                            request: request.clone(),
                            locator: locator.clone(),
                        },
                    )
                    .await?;
                (locator, granted)
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        let challenge_context = request.cross_challenge_context();
        let probe_id = coven_protocol::provider::ProviderProbeId::from_bytes(
            *ObjectHash::digest(database.new_store_write_id().as_str().as_bytes()).as_bytes(),
        );
        let challenge = self
            .storage
            .prepare_cross_principal_challenge(
                &database,
                probe_id,
                &request.offer.provider,
                &challenge_context,
                self.local_writer.as_ref(),
            )
            .await
            .map_err(DeviceJoinError::ProviderProbe)?;
        let approval = self.sign_device_admission_approval(
            request,
            DeviceProviderAdmission::CrossPrincipal { locator, challenge },
        )?;
        journal
            .advance(
                &granted,
                OwnerJoinProgress::ApprovalPrepared(approval.clone()),
            )
            .await?;
        Ok(approval)
    }

    pub(crate) async fn publish_challenge(
        &mut self,
        bootstrap: ProvisionalDeviceBootstrap,
    ) -> Result<ProviderReadyDeviceBootstrap, DeviceJoinError> {
        let offer = &bootstrap.request.approval().request.offer;
        if &self.resolve_provider_admin(&offer.provider_admin.grant_id)?
            != offer.provider_admin.as_ref()
        {
            return Err(DeviceJoinError::OfferMismatch);
        }
        let owner = self
            .join_history()
            .load_registration(&offer.owner_registration)
            .await?
            .value;
        self.local_writer.verify_own_device_admission_approval(
            bootstrap.request.approval(),
            &self.verified_root,
        )?;
        let challenge_publication = match &bootstrap.request.approval().admission {
            DeviceProviderAdmission::SamePrincipal => {
                DeviceProviderChallengePublication::SamePrincipal
            }
            DeviceProviderAdmission::CrossPrincipal { challenge, .. } => {
                let context = bootstrap
                    .request
                    .approval()
                    .request
                    .cross_challenge_context();
                let authorization = DeviceJoinChallengePublicationAuthorization {
                    attempt_id: bootstrap.publication_authorization.attempt_id,
                    attempt_activation: bootstrap
                        .publication_authorization
                        .attempt_activation
                        .clone(),
                };
                let published = self
                    .publish_cross_principal_challenge(
                        &authorization,
                        challenge,
                        &context,
                        &offer.provider,
                        &owner,
                    )
                    .await?;
                DeviceProviderChallengePublication::CrossPrincipal {
                    challenge: published,
                }
            }
        };
        let attempt_id = offer.attempt_id;
        let ready = ProviderReadyDeviceBootstrap {
            bootstrap: Box::new(bootstrap),
            challenge_publication,
        };
        let journal = self.journal(attempt_id);
        let current = journal.current().await?;
        match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ProviderReady(existing))
                if existing == &ready =>
            {
                return Ok(ready)
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap))
                if bootstrap == ready.bootstrap.as_ref() =>
            {
                let intent = journal
                    .advance(
                        &current,
                        OwnerJoinProgress::ChallengeCreateIntent(*ready.bootstrap.clone()),
                    )
                    .await?;
                journal
                    .advance(&intent, OwnerJoinProgress::ProviderReady(ready.clone()))
                    .await?;
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ChallengeCreateIntent(bootstrap))
                if bootstrap == ready.bootstrap.as_ref() =>
            {
                journal
                    .advance(&current, OwnerJoinProgress::ProviderReady(ready.clone()))
                    .await?;
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        }
        Ok(ready)
    }

    pub(super) async fn complete_admission(
        &mut self,
        readiness: DeviceJoinReadiness,
    ) -> Result<DeviceProviderAdmissionCompletion, DeviceJoinError> {
        let attempt_id = readiness.proof.attempt_id;
        let database = self.database.clone();
        let journal = self.journal(attempt_id);
        let current = journal.current().await?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Completed(existing)) =
            &*current.progress
        {
            if matches!(
                existing,
                DeviceProviderAdmissionCompletion::CrossPrincipal {
                    readiness: durable,
                    ..
                } if **durable == readiness
            ) {
                return Ok(existing.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        let bootstrap = match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ProviderReady(bootstrap)) => {
                bootstrap.clone()
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        if readiness.proof.attempt_id != bootstrap.bootstrap.publication_authorization.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let offer = &bootstrap.bootstrap.request.approval().request.offer;
        let provider_admin = self.resolve_provider_admin(&offer.provider_admin.grant_id)?;
        if &provider_admin != offer.provider_admin.as_ref() {
            return Err(DeviceJoinError::ProviderAdministratorRequired);
        }
        let receipt = match (
            &bootstrap.bootstrap.request.approval().admission,
            &bootstrap.bootstrap.request.response(),
            &readiness.provider,
        ) {
            (
                DeviceProviderAdmission::CrossPrincipal { challenge, .. },
                DeviceProviderResponseReservation::CrossPrincipal { response_slot },
                DeviceProviderReadiness::CrossPrincipal(response),
            ) => {
                let context = coven_protocol::provider::CrossPrincipalResponseContext {
                    challenge: bootstrap
                        .bootstrap
                        .request
                        .approval()
                        .request
                        .cross_challenge_context(),
                    expected_registration_hash: bootstrap
                        .bootstrap
                        .request
                        .expected_registration()
                        .registration_hash(),
                    response_slot: response_slot.clone(),
                };
                self.storage
                    .complete_cross_principal_probe(
                        &database,
                        challenge,
                        response,
                        &context,
                        &offer.provider,
                        self.local_writer.as_ref(),
                        &offer.member_pubkey,
                    )
                    .await
                    .map_err(DeviceJoinError::ProviderProbe)?
            }
            _ => return Err(DeviceJoinError::AttemptMismatch),
        };
        let completion = DeviceProviderAdmissionCompletion::CrossPrincipal {
            bootstrap: Box::new(bootstrap.clone()),
            readiness: Box::new(readiness.clone()),
            receipt,
        };
        let observed = journal
            .advance(&current, OwnerJoinProgress::ResponseObserved(readiness))
            .await?;
        journal
            .advance(&observed, OwnerJoinProgress::Completed(completion.clone()))
            .await?;
        Ok(completion)
    }

    pub(crate) async fn complete_same_principal(
        &mut self,
        bootstrap: ProviderReadyDeviceBootstrap,
    ) -> Result<DeviceProviderAdmissionCompletion, DeviceJoinError> {
        let attempt_id = bootstrap.bootstrap.publication_authorization.attempt_id;
        if !matches!(
            (
                &bootstrap.bootstrap.request.approval().admission,
                bootstrap.bootstrap.request.response(),
                &bootstrap.challenge_publication,
            ),
            (
                DeviceProviderAdmission::SamePrincipal,
                DeviceProviderResponseReservation::SamePrincipal,
                DeviceProviderChallengePublication::SamePrincipal,
            )
        ) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let journal = self.journal(attempt_id);
        let current = journal.current().await?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Completed(existing)) =
            &*current.progress
        {
            return match existing {
                DeviceProviderAdmissionCompletion::SamePrincipal { bootstrap: durable }
                    if **durable == bootstrap =>
                {
                    Ok(existing.clone())
                }
                _ => Err(DeviceJoinError::JournalConflict),
            };
        }
        match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ProviderReady(durable))
                if durable == &bootstrap => {}
            _ => return Err(DeviceJoinError::JournalConflict),
        }
        let completion = DeviceProviderAdmissionCompletion::SamePrincipal {
            bootstrap: Box::new(bootstrap),
        };
        journal
            .advance(&current, OwnerJoinProgress::Completed(completion.clone()))
            .await?;
        Ok(completion)
    }
}

impl Store {
    #[doc(hidden)]
    pub(crate) async fn authorize_device_provider_access(
        &self,
        request: DeviceProviderAccessRequest,
        access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
    ) -> Result<DeviceProviderAdmissionApproval, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer
            .join_operation()
            .authorize_access(request, access_administrator)
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn publish_device_provider_challenge(
        &self,
        bootstrap: ProvisionalDeviceBootstrap,
    ) -> Result<ProviderReadyDeviceBootstrap, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer.join_operation().publish_challenge(bootstrap).await
    }

    #[doc(hidden)]
    pub(crate) async fn complete_device_provider_admission(
        &self,
        readiness: DeviceJoinReadiness,
    ) -> Result<DeviceProviderAdmissionCompletion, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer.join_operation().complete_admission(readiness).await
    }

    #[doc(hidden)]
    pub(crate) async fn complete_same_principal_device_admission(
        &self,
        bootstrap: ProviderReadyDeviceBootstrap,
    ) -> Result<DeviceProviderAdmissionCompletion, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer
            .join_operation()
            .complete_same_principal(bootstrap)
            .await
    }
}

#[async_trait::async_trait]
pub trait DeviceProviderAccessAdministrator: Send + Sync {
    async fn grant_member_access(
        &self,
        member_pubkey: &str,
        provider_account_email: Option<&str>,
        peer: &ProviderDeviceBinding,
    ) -> Result<coven_protocol::provider::ProviderAccessLocator, DeviceJoinError>;
}
