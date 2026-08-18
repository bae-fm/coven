use super::*;

impl<'operation, 'storage> AuthorizedJoin<'operation, 'storage> {
    /// Activate a same-provider join as one Store operation. The attempt,
    /// registration, and outcome are one indivisible commit.
    pub(crate) async fn activate_same_principal_join(
        &mut self,
        request: DeviceRegistrationRequest,
    ) -> Result<SamePrincipalDeviceJoin, DeviceJoinError> {
        if !matches!(request, DeviceRegistrationRequest::SamePrincipal { .. }) {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        let offer = self.validate_registration_request(&request).await?;
        let journal = self.journal(offer.attempt_id);
        let current = journal.current().await?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::SamePrincipalCompleted { join }) =
            &*current.progress
        {
            return Ok(join.clone());
        }

        let requested =
            match &*current.progress {
                DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(durable))
                    if durable == &offer =>
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

        let plan = self.writer.prepare_plan().await?;
        #[cfg(any(test, feature = "test-utils"))]
        self.database
            .reach_test_point(coven_database::DatabaseTestPoint::DeviceJoinAttemptPositionHeld)
            .await;
        let plan_cut = plan.predecessor_cut()?;
        let plan_membership = plan.membership_state().clone();

        let attempt_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let attempt_prefix =
            coven_protocol::store_commit::device_join_attempt_semantic_prefix(offer.attempt_id);
        let registration_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let registration_prefix = coven_protocol::store_commit::registration_semantic_prefix(
            &request.expected_registration().device_id.to_string(),
        );
        let outcome_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinOutcome,
        );
        let outcome_prefix =
            coven_protocol::store_commit::device_join_outcome_semantic_prefix(offer.attempt_id);

        let (
            attempt_ref,
            attempt_prepared,
            registration_ref,
            registration_prepared,
            outcome_ref,
            outcome_prepared,
            intent,
        ) = match &*requested.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(_)) => {
                let attempt = self.local_writer.sign_device_join_attempt(
                    offer.store_root.clone(),
                    offer.attempt_id,
                    offer.attempt_slot.clone(),
                    request.expected_registration().clone(),
                    request.registration_slot().clone(),
                    offer.outcome_slot.clone(),
                    plan_cut.clone(),
                    plan_membership.clone(),
                    offer.provider_admin.grant_id.clone(),
                    request.approval().clone(),
                    request.response(),
                    offer.owner_grant.clone(),
                )?;
                let attempt_prepared = self.storage.prepare_protocol_object(
                    &attempt_context,
                    offer.attempt_slot.clone(),
                    &attempt_prefix,
                    attempt.to_bytes(),
                )?;
                let attempt_ref = DeviceJoinAttemptRef {
                    attempt_id: offer.attempt_id,
                    attempt_hash: attempt.attempt_hash(),
                    object: attempt_prepared.reference().clone(),
                };
                let registration_prepared = super::prepare_registration_object(
                    self.storage.as_ref(),
                    request.expected_registration(),
                    request.registration_slot().clone(),
                )?;
                let registration_ref = StoreDeviceRegistrationRef::from_registration(
                    request.expected_registration(),
                    registration_prepared.reference().clone(),
                );
                let outcome = self.local_writer.sign_device_join_outcome(
                    attempt_ref.clone(),
                    coven_protocol::store_commit::DeviceJoinDisposition::Activated {
                        registration: registration_ref.clone(),
                    },
                    offer.owner_grant.clone(),
                )?;
                let outcome_prepared = self.storage.prepare_protocol_object(
                    &outcome_context,
                    offer.outcome_slot.clone(),
                    &outcome_prefix,
                    outcome.to_bytes(),
                )?;
                let outcome_ref = DeviceJoinOutcomeRef::Activated {
                    attempt: attempt_ref.clone(),
                    outcome_hash: outcome.outcome_hash(),
                    object: outcome_prepared.reference().clone(),
                };
                let intent = journal
                    .advance(
                        &requested,
                        OwnerJoinProgress::SamePrincipalActivationCreateIntent {
                            request: request.clone(),
                            bootstrap_cut: plan_cut.clone(),
                            membership: plan_membership.clone(),
                            attempt: attempt_ref.clone(),
                            attempt_prepared: PreparedDeviceJoinObject::from_prepared(
                                &attempt_prepared,
                            ),
                            registration: registration_ref.clone(),
                            registration_prepared: PreparedDeviceJoinObject::from_prepared(
                                &registration_prepared,
                            ),
                            outcome: outcome_ref.clone(),
                            outcome_prepared: PreparedDeviceJoinObject::from_prepared(
                                &outcome_prepared,
                            ),
                        },
                    )
                    .await?;
                (
                    attempt_ref,
                    attempt_prepared,
                    registration_ref,
                    registration_prepared,
                    outcome_ref,
                    outcome_prepared,
                    intent,
                )
            }
            DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::SamePrincipalActivationCreateIntent {
                    bootstrap_cut,
                    membership,
                    attempt,
                    attempt_prepared,
                    registration,
                    registration_prepared,
                    outcome,
                    outcome_prepared,
                    ..
                },
            ) => {
                if bootstrap_cut != &plan_cut || membership != &plan_membership {
                    return Err(DeviceJoinError::JournalConflict);
                }
                (
                    attempt.clone(),
                    attempt_prepared.restore()?,
                    registration.clone(),
                    registration_prepared.restore()?,
                    outcome.clone(),
                    outcome_prepared.restore()?,
                    requested.clone(),
                )
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };

        let attempt = self.local_writer.sign_device_join_attempt(
            offer.store_root.clone(),
            offer.attempt_id,
            offer.attempt_slot.clone(),
            request.expected_registration().clone(),
            request.registration_slot().clone(),
            offer.outcome_slot.clone(),
            plan_cut,
            plan_membership,
            offer.provider_admin.grant_id.clone(),
            request.approval().clone(),
            request.response(),
            offer.owner_grant.clone(),
        )?;
        let outcome = self.local_writer.sign_device_join_outcome(
            attempt_ref.clone(),
            coven_protocol::store_commit::DeviceJoinDisposition::Activated {
                registration: registration_ref.clone(),
            },
            offer.owner_grant.clone(),
        )?;
        outcome_ref.verify_outcome(&outcome)?;
        if attempt_ref.attempt_hash != attempt.attempt_hash()
            || attempt_prepared.reference() != &attempt_ref.object
            || attempt_prepared.stored_bytes() != attempt.to_bytes()
            || registration_prepared.reference() != &registration_ref.object
            || registration_prepared.stored_bytes() != request.expected_registration().to_bytes()
            || outcome_ref.object() != outcome_prepared.reference()
            || outcome_prepared.stored_bytes() != outcome.to_bytes()
        {
            return Err(DeviceJoinError::JournalConflict);
        }

        let attempt_bytes = attempt.to_bytes();
        let registration_bytes = request.expected_registration().to_bytes();
        let outcome_bytes = outcome.to_bytes();
        let activated_registration =
            coven_protocol::store_commit::ActivatedStoreDeviceRegistration::verified(
                coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
                    registration_ref,
                    request.expected_registration().clone(),
                )?,
                coven_protocol::store_commit::StoreDeviceRegistrationActivation::Join {
                    attempt_id: offer.attempt_id,
                    outcome: outcome_ref.clone(),
                },
            )?;
        let candidate = self
            .writer
            .prepare_candidate(
                plan,
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::SamePrincipalDeviceJoin {
                    attempt: attempt_ref.clone(),
                    outcome: outcome_ref.clone(),
                    registration: Box::new(activated_registration),
                },
            )
            .await?;
        let create_attempt = self.storage.create_verified_protocol_object(
            &attempt_context,
            &attempt_prepared,
            &attempt_prefix,
            &attempt_bytes,
        );
        let create_registration = self.storage.create_verified_protocol_object(
            &registration_context,
            &registration_prepared,
            &registration_prefix,
            &registration_bytes,
        );
        let create_outcome = self.storage.create_verified_protocol_object(
            &outcome_context,
            &outcome_prepared,
            &outcome_prefix,
            &outcome_bytes,
        );
        let upload_commit = self.writer.upload_prepared(Box::new(candidate));
        let ((), (), (), uploaded) = tokio::try_join!(
            async { create_attempt.await.map_err(DeviceJoinError::Storage) },
            async { create_registration.await.map_err(DeviceJoinError::Storage) },
            async { create_outcome.await.map_err(DeviceJoinError::Storage) },
            async { upload_commit.await.map_err(DeviceJoinError::from) },
        )?;
        let activation_ref = self.writer.activate_uploaded(uploaded).await?;
        self.join_history()
            .retain_same_principal_join_activation(&activation_ref)
            .await?;
        let bootstrap = ProviderReadyDeviceBootstrap {
            bootstrap: Box::new(ProvisionalDeviceBootstrap {
                request: Box::new(request),
                publication_authorization: DeviceJoinChallengePublicationAuthorization {
                    attempt: attempt_ref,
                    attempt_activation: activation_ref.clone(),
                },
            }),
            challenge_publication: DeviceProviderChallengePublication::SamePrincipal,
        };
        let activation = DeviceJoinActivation {
            outcome: outcome_ref,
            outcome_activation: activation_ref,
        };
        let installation = self
            .join_history()
            .prepare_same_principal_installation(&attempt, outcome, &activation.outcome_activation)
            .await?;
        let join = SamePrincipalDeviceJoin::verified(bootstrap, activation, installation)?;
        journal
            .advance(
                &intent,
                OwnerJoinProgress::SamePrincipalCompleted { join: join.clone() },
            )
            .await?;
        Ok(join)
    }
}
