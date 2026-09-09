use super::*;
use crate::sync::store::authorization::history::membership_publication::MembershipPublicationSigner;

impl RestoringStore<'_> {
    pub(super) async fn publish_owner_recovery(
        &mut self,
        publication: coven_database::OwnerRecoveryPublication,
    ) -> Result<StoreDeviceRegistrationRef, StoreRegistrationError> {
        let registration =
            coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
                publication.commit.value.author_registration.clone(),
                publication.commit.value.author().clone(),
            )?;
        let device_signer = registration.value().device_signer(&self.identity)?;
        let signer = MembershipPublicationSigner::owner_recovery(
            &registration,
            &device_signer,
            &self.identity,
        )?;
        let membership_publication = publication.membership_publication()?;
        for remote in publication.remote_objects()? {
            let prepared = if remote.record().object() == &membership_publication.entry_ref.object {
                membership_publication
                    .prepared_entry()
                    .map_err(StoreError::from)?
            } else if remote.record().object() == &membership_publication.head_ref.object {
                membership_publication
                    .prepared_head()
                    .map_err(StoreError::from)?
            } else if remote.record().object() == &publication.commit.value.reference().object {
                publication.commit.prepared.clone()
            } else {
                return Err(StoreRegistrationError::Invalid(
                    "Owner recovery remote graph contains an unrelated exact object".into(),
                ));
            };
            self.storage
                .create_protocol_object(&prepared)
                .await
                .map_err(StoreObjectError::from)?;
            if remote.record().object() == &publication.commit.value.reference().object {
                self.database
                    .mark_candidate_commit_uploaded(publication.commit.value.reference().clone())
                    .await?;
            } else {
                self.database
                    .mark_remote_object_uploaded(remote.record().clone())
                    .await?;
            }
        }
        self.history
            .membership_objects()
            .load_head(&membership_publication.head_ref)
            .await?;
        let outcome = self
            .history
            .publish_store_commit(
                &mut self.membership,
                &self.identity,
                &device_signer,
                &publication.commit.value,
            )
            .await?
            .require_published()?;
        let proof = publication
            .history_evidence
            .membership_proof
            .as_ref()
            .ok_or_else(|| {
                StoreRegistrationError::Invalid(
                    "Owner recovery lost its exact authority proof".into(),
                )
            })?;
        let result = self
            .history
            .finalize_membership_head_acceptance(
                &signer,
                &publication.commit.value,
                proof,
                &outcome,
            )
            .await?;
        let [activation] = publication.commit.value.device_registrations() else {
            return Err(StoreRegistrationError::Invalid(
                "Owner recovery has no sole registration activation".into(),
            ));
        };
        let StoreDeviceRegistrationActivationRef::Recovery { recovery_id, node } =
            &activation.authority
        else {
            return Err(StoreRegistrationError::Invalid(
                "Owner recovery carries another registration authority".into(),
            ));
        };
        let registration_ref = registration.reference().clone();
        let activated = coven_protocol::store_commit::ActivatedStoreDeviceRegistration::verified(
            registration,
            StoreDeviceRegistrationActivation::Recovery {
                recovery_id: *recovery_id,
                node: node.clone(),
            },
        )?;
        self.database
            .complete_owner_recovery(
                publication.commit.value,
                outcome,
                publication.history_evidence,
                activated,
                result,
            )
            .await?;
        Ok(registration_ref)
    }
}
