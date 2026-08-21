use super::*;

impl<'operation, 'storage> AuthorizedJoin<'operation, 'storage> {
    /// Activate a same-provider join as one Store operation. The attempt,
    /// registration, and outcome are one indivisible commit.
    pub(crate) async fn activate_same_principal_join(
        &mut self,
        request: DeviceRegistrationRequest,
    ) -> Result<SamePrincipalDeviceJoin, DeviceJoinError> {
        // Most of a live same-provider admission is spent here, and from the
        // transport step above it is one opaque span. The stages below are the
        // pieces: two history walks, the signing, the four uploads, and the
        // journal write that carries the whole join.
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
            .map_err(DeviceJoinError::from)?;
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

        let requested =
            match &*current.progress {
                DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ApprovalPrepared(approval))
                    if approval == request.approval() =>
                {
                    journal
                        .advance(
                            &current,
                            OwnerJoinProgress::RegistrationRequested(request.clone()),
                        )
                        .await?
                }
                DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(
                    durable,
                )) if durable == &request => current.clone(),
                DeviceJoinRoleProgress::Owner(
                    OwnerJoinProgress::SamePrincipalActivationCreateIntent {
                        request: durable, ..
                    },
                ) if durable == &request => current.clone(),
                _ => return Err(DeviceJoinError::JournalConflict),
            };

        let plan = timings
            .stage("prepare the commit plan", self.writer.prepare_plan())
            .await?;
        #[cfg(any(test, feature = "test-utils"))]
        self.database
            .reach_test_point(coven_database::DatabaseTestPoint::DeviceJoinAttemptPositionHeld)
            .await;
        let plan_cut = plan.predecessor_cut()?;
        let plan_membership = plan.membership_state().clone();

        let registration_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let registration_prefix = coven_protocol::store_commit::registration_semantic_prefix(
            &request.expected_registration().device_id.to_string(),
        );
        let (registration_ref, registration_prepared, intent) = match &*requested.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(_)) => {
                let registration_prepared = super::prepare_registration_object(
                    self.storage.as_ref(),
                    request.expected_registration(),
                    request.registration_slot().clone(),
                )?;
                let registration_ref = StoreDeviceRegistrationRef::from_registration(
                    request.expected_registration(),
                    registration_prepared.reference().clone(),
                );
                let intent = journal
                    .advance(
                        &requested,
                        OwnerJoinProgress::SamePrincipalActivationCreateIntent {
                            request: request.clone(),
                            bootstrap_cut: plan_cut.clone(),
                            membership: plan_membership.clone(),
                            registration: registration_ref.clone(),
                            registration_prepared: PreparedDeviceJoinObject::from_prepared(
                                &registration_prepared,
                            ),
                        },
                    )
                    .await?;
                (registration_ref, registration_prepared, intent)
            }
            DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::SamePrincipalActivationCreateIntent {
                    bootstrap_cut,
                    membership,
                    registration,
                    registration_prepared,
                    ..
                },
            ) => {
                if bootstrap_cut != &plan_cut || membership != &plan_membership {
                    return Err(DeviceJoinError::JournalConflict);
                }
                (
                    registration.clone(),
                    registration_prepared.restore()?,
                    requested.clone(),
                )
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };

        if registration_prepared.reference() != &registration_ref.object
            || registration_prepared.stored_bytes() != request.expected_registration().to_bytes()
        {
            return Err(DeviceJoinError::JournalConflict);
        }
        let registration_bytes = request.expected_registration().to_bytes();
        let activated_registration =
            coven_protocol::store_commit::ActivatedStoreDeviceRegistration::verified(
                coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
                    registration_ref.clone(),
                    request.expected_registration().clone(),
                )?,
                coven_protocol::store_commit::StoreDeviceRegistrationActivation::Join {
                    attempt_id: offer.attempt_id,
                },
            )?;
        let candidate = timings
            .stage(
                "prepare the candidate commit",
                self.writer.prepare_candidate(
                plan,
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::SamePrincipalDeviceJoin {
                    attempt_id: offer.attempt_id,
                    registration: Box::new(activated_registration),
                },
                ),
            )
            .await?;
        let create_registration = self.storage.create_verified_protocol_object(
            &registration_context,
            &registration_prepared,
            &registration_prefix,
            &registration_bytes,
        );
        let upload_commit = self.writer.upload_prepared(Box::new(candidate));
        let ((), uploaded) = timings
            .stage("publish the join objects", async {
                tokio::try_join!(
                    async { create_registration.await.map_err(DeviceJoinError::Storage) },
                    async { upload_commit.await.map_err(DeviceJoinError::from) },
                )
            })
            .await?;
        let activation_ref = timings
            .stage(
                "activate the uploaded commit",
                self.writer.activate_uploaded(uploaded),
            )
            .await?;
        timings
            .stage(
                "retain the activation",
                self.join_history()
                    .retain_same_principal_join_activation(&activation_ref),
            )
            .await?;
        let bootstrap = ProviderReadyDeviceBootstrap {
            bootstrap: Box::new(ProvisionalDeviceBootstrap {
                request: Box::new(request),
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
        // Selecting the snapshot and preparing the carried closure are both
        // history walks, and neither is visible from the commit publication in
        // front of them.
        let installation = timings
            .stage(
                "prepare the installation",
                self.join_history()
                    .prepare_same_principal_installation(&activation.outcome_activation),
            )
            .await?;
        let join = SamePrincipalDeviceJoin::verified(bootstrap, activation, installation)?;
        // The completed join goes into one journal row, carried closure and
        // all, so this write scales with what that closure carries.
        timings
            .stage(
                "journal the completion",
                journal.advance(
                    &intent,
                    OwnerJoinProgress::SamePrincipalCompleted {
                        join: join.clone(),
                        registration: registration_ref.clone(),
                    },
                ),
            )
            .await?;
        Ok(join)
    }
}
