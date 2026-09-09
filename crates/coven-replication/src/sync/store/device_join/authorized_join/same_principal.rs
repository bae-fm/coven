use super::*;

impl<'operation, 'storage> AuthorizedJoin<'operation, 'storage> {
    pub(crate) async fn activate_same_principal_join(
        &mut self,
        request: DeviceRegistrationRequest,
    ) -> Result<SamePrincipalDeviceJoin, DeviceJoinError> {
        let mut timings = coven_foundation::stage_timing::StageTimings::counting(
            "Same-provider join activation",
            self.storage.provider_requests(),
        );
        let outcome =
            Box::pin(self.activate_same_principal_join_staged(request, &mut timings)).await;
        timings.report();
        outcome
    }

    async fn activate_same_principal_join_staged(
        &mut self,
        request: DeviceRegistrationRequest,
        timings: &mut coven_foundation::stage_timing::StageTimings,
    ) -> Result<SamePrincipalDeviceJoin, DeviceJoinError> {
        if !matches!(request, DeviceRegistrationRequest::SamePrincipal { .. }) {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        timings
            .stage("seed retained history", self.writer.seed_retained_history())
            .await
            .map_err(|error| {
                DeviceJoinError::from(crate::sync::store::pull::StorePullError::context(
                    "seed retained history for device join",
                    error,
                ))
            })?;
        let offer = timings
            .stage(
                "validate the request",
                self.validate_registration_request(&request),
            )
            .await?;
        let journal = self.journal(offer.attempt_id);
        let current = timings.stage("read the journal", journal.current()).await?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::SamePrincipalCompleted {
            join,
            ..
        }) = &*current.progress
        {
            return Ok(join.clone());
        }

        let (activation_ref, registration_ref) = match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::SamePrincipalActivated {
                request: durable,
                registration,
                activation,
                ..
            }) if durable == &request => (activation.clone(), registration.clone()),
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::StorePublicationPrepared(
                prepared,
            )) if matches!(&prepared.operation, OwnerJoinPublication::SamePrincipalActivation { request: durable } if durable == &request) =>
            {
                let registration = prepared
                    .candidate
                    .registration_activation
                    .as_ref()
                    .ok_or(DeviceJoinError::JournalConflict)?
                    .reference()
                    .clone();
                let activation = timings
                    .stage(
                        "publish the join",
                        self.publish_owner_publication(prepared.clone()),
                    )
                    .await?;
                (activation, registration)
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ApprovalPrepared(approval))
                if approval == request.approval() =>
            {
                let requested = journal
                    .advance(
                        &current,
                        OwnerJoinProgress::RegistrationRequested(request.clone()),
                    )
                    .await?;
                self.prepare_and_publish_same_principal(
                    requested,
                    request.clone(),
                    offer.attempt_id,
                    timings,
                )
                .await?
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(durable))
                if durable == &request =>
            {
                self.prepare_and_publish_same_principal(
                    current,
                    request.clone(),
                    offer.attempt_id,
                    timings,
                )
                .await?
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };

        let accepted = journal.current().await?;
        let accepted_current = match &*accepted.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::SamePrincipalActivated {
                request: durable,
                registration,
                activation,
                accepted_current,
            }) if durable == &request
                && registration == &registration_ref
                && activation == &activation_ref =>
            {
                accepted_current.clone()
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        let bootstrap = ProviderReadyDeviceBootstrap {
            bootstrap: Box::new(ProvisionalDeviceBootstrap {
                request: Box::new(request.clone()),
                publication_authorization: DeviceJoinChallengePublicationAuthorization {
                    attempt_id: offer.attempt_id,
                    attempt_activation: activation_ref.clone(),
                },
            }),
            challenge_publication: DeviceProviderChallengePublication::SamePrincipal,
        };
        let activation = DeviceJoinActivation {
            attempt_id: offer.attempt_id,
            outcome_activation: activation_ref,
        };
        let installation = timings
            .stage(
                "prepare the installation",
                self.join_history().prepare_same_principal_installation(
                    &activation.outcome_activation,
                    accepted_current,
                ),
            )
            .await?;
        let join = SamePrincipalDeviceJoin::verified(bootstrap, activation, installation)?;
        timings
            .stage(
                "journal the completion",
                journal.advance(
                    &accepted,
                    OwnerJoinProgress::SamePrincipalCompleted {
                        join: join.clone(),
                        registration: registration_ref,
                    },
                ),
            )
            .await?;
        Ok(join)
    }

    async fn prepare_and_publish_same_principal(
        &mut self,
        requested: DeviceJoinJournalRecord,
        request: DeviceRegistrationRequest,
        attempt_id: DeviceJoinAttemptId,
        timings: &mut coven_foundation::stage_timing::StageTimings,
    ) -> Result<(StoreBatchCommitRef, StoreDeviceRegistrationRef), DeviceJoinError> {
        self.writer.refresh_membership_publication().await?;
        let plan = timings
            .stage("prepare the commit plan", self.writer.prepare_plan())
            .await?;
        #[cfg(any(test, feature = "test-utils"))]
        self.database
            .reach_test_point(coven_database::DatabaseTestPoint::DeviceJoinAttemptPositionHeld)
            .await;
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
                    registration_ref.clone(),
                    request.expected_registration().clone(),
                )?,
                coven_protocol::store_commit::StoreDeviceRegistrationActivation::Join {
                    attempt_id,
                },
            )?;
        let publication = self
            .prepare_registration_publication(
                requested,
                OwnerJoinPublication::SamePrincipalActivation {
                    request: request.clone(),
                },
                plan,
                activated_registration,
            )
            .await?;
        let activation = timings
            .stage(
                "publish the join",
                self.publish_owner_publication(publication),
            )
            .await?;
        Ok((activation, registration_ref))
    }
}
